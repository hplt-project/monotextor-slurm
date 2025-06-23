import sys
import os
import io
import argparse
import logging
import timeit
import orjson
import torch
from typing import List
from transformers import AutoModelForSequenceClassification, AutoTokenizer

def logging_setup(args = None):
    logger = logging.getLogger()
    logger.handlers = [] # Removing default handler to avoid duplication of log messages
    logger.setLevel(logging.ERROR)

    h = logging.StreamHandler(sys.stderr)
    if args != None:
        h = logging.StreamHandler(args.logfile)

    h.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(h)

    #logger.setLevel(logging.INFO)

    if args != None:
        if not args.quiet:
            logger.setLevel(logging.INFO)
        if args.debug:
            logger.setLevel(logging.DEBUG)

def initialization():
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]), formatter_class=argparse.ArgumentDefaultsHelpFormatter, description=__doc__)
    parser.add_argument('input',  nargs='?', type=argparse.FileType('rt', errors="replace"), default=io.TextIOWrapper(sys.stdin.buffer, errors="replace"),  help="Input sentences.")
    parser.add_argument('output', nargs='?', type=argparse.FileType('wt'), default=sys.stdout, help="Output of the register identification.")

    groupO = parser.add_argument_group("Options")
    groupO.add_argument("--field", type=str, default="text", help="Name of the JSON field that contains the text to be analyzed")
    groupO.add_argument("--raw", action="store_true", help="True if the input is already raw, non-json text")
    groupO.add_argument("-b", "--mini-batch", type=int, default=16, help="Mini batch size")
    groupO.add_argument("-B", "--maxi-batch", type=int, default=10000, help="Maxi batch size")

    groupL = parser.add_argument_group('Logging')
    groupL.add_argument('-q', '--quiet', action='store_true', help='Silent logging mode')
    groupL.add_argument('--debug', action='store_true', help='Debug logging mode')
    groupL.add_argument('--info', action='store_true', help='Info logging mode')
    groupL.add_argument('--logfile', type=argparse.FileType('a'), default=sys.stderr, help="Store log to a file")
    #groupL.add_argument('-v', '--version', action='version', version="%(prog)s " + __version__, help="show version of this script and exit")

    args = parser.parse_args()
    logging_setup(args)
    return args

#logging.basicConfig(level=logging.DEBUG)

class RegisterLabels:
    def __init__(self):
        #supported languages; https://github.com/facebookresearch/fairseq/tree/main/examples/xlmr
        if torch.cuda.is_available():
            self.device = torch.device("cuda")
        else:
            logging.error("No GPU device available")
            sys.exit(1)
        self.model_id = "TurkuNLP/multilingual-web-register-classification"

        # Load model and tokenizer
        self.model = AutoModelForSequenceClassification.from_pretrained(self.model_id).to(self.device)
        self.id2label = self.model.config.id2label
        logging.info ("Model loaded")
        self.tokenizer = AutoTokenizer.from_pretrained("xlm-roberta-large")
        logging.info("Tokenizer loaded")

    def get_labels(self, docs: List[dict]):
        # Tokenize text
        inputs = self.tokenizer(
                [i["text"] for i in docs],
                return_tensors="pt",
                padding=True,
                truncation=True,
                max_length=512).to(self.device)

        with torch.no_grad(), torch.autocast(device_type=self.device.type, dtype=torch.float16):
            outputs = self.model(**inputs)
        logging.debug(outputs)

        # Apply sigmoid to the logits to get probabilities
        probabilities = torch.sigmoid(outputs.logits).squeeze()
        logging.debug(probabilities)

        # Extract readable labels using id2label
        def map_prob_label(sample):
            for i, prob in enumerate(sample):
                yield self.id2label[i], round(prob, 3)

        predicted_labels = [dict(map_prob_label(i)) for i in probabilities.tolist()]

        return predicted_labels

def read_batches(input_file, size):
    batch = []
    for line in input_file:
        if len(batch) >= size:
            yield batch
            batch = []
        doc = orjson.loads(line)
        batch.append(doc)

    if len(batch) > 0:
        yield batch

def perform_identification(args):
    time_start = timeit.default_timer()
    rl = RegisterLabels()
    docs = 0
    for batch in read_batches(args.input, args.mini_batch):
        docs += len(batch)
        labels = rl.get_labels(batch)
        logging.debug(labels)
        for doc, l in zip(batch, labels):
            doc["web-register"] = l
            serialized = orjson.dumps(
                doc,
                option=(
                    orjson.OPT_SERIALIZE_NUMPY |
                    orjson.OPT_APPEND_NEWLINE
                )).decode()
            args.output.write(serialized)
        #for l in labels: #one label, or two if MT is one of them
        #    args.output.write(l.strip()+"\n")

    elapsed_time = timeit.default_timer() - time_start
    logging.info("Total: {0} docs".format(docs))
    logging.info("Elapsed time {0:.2f} s".format(elapsed_time))
    logging.info("Troughput: {0} docs/s".format(int((docs*1.0)/elapsed_time)))

def main():
    logging_setup()
    args = initialization() # Parsing parameters
    logging.info("Executing main program...")
    perform_identification(args)
    logging.info("Program finished")

if __name__ == '__main__':
    main()

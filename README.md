# monotextor-slurm
Set of scripts to run monotextor-like pipeline under slurm HPCs 


## Pipeline description
### Merge-batching
This pipeline reads from directories containing files following the [warc2text](https://github.com/bitextor/warc2text) output format.
Specifically, it reads from each directory the `url.gz` and `plain_text.gz` files,
decoding each document in base64 to plain text and writing as output a tab separated file where each line contains a url, paragraph and metadata.
```
url1    this is paragraph1      collection1
url1    this is paragraph2      collection1
url1    this is paragraph3      collection1
url2    another paragraph       collection1
url2
...
```
Right now, the first step of the pipeline expects input directory structure sharded with [giashard](https://github.com/paracrawl/giashard), but any other structure can be used changing the [00.merge-batching](https://github.com/hplt-project/monotextor-slurm/blob/2dc38e1b822b69f5405fa753aa1fb9065ac8201a/00.merge-batching#L48) listing of directories.
As far as each process in `parallel` receives a directory containing the files above mentioned.

After creating the TSV, files are divided again into batches of similar size and balance the scheduling and parallelization of the processing step.

### Processing
The processing step consists of three parts:
 - [monofixer](https://github.com/bitextor/bifixer) to fix encoding issues and remove html entities.
 - [monocleaner](https://github.com/bitextor/monocleaner) to add two metadata columns:
   - Language identified by [fastspell](https://github.com/mbanon/fastspell).
   - Character fluency score provided by 7-gram character language models.
 - Conversion to JSONL format where each line is a document in JSON format with all the metadata.

In this step, an array job of size number of batches is run.
Each batch file is processed with one job that allocates a full node and parallelizes processing by lines.
After that, another job array is submitted where each job is a serial job that converts TSV to JSONL.

### Deduplication
TBD


## Install
Install requirements inside your virtual environment.
```
pip install -r requirements.txt
```

## Configure
Copy the `.env.example` to `.env` and edit the variables accordingly.
Needed variables are:
```
SBATCH_ACCOUNT          Project account number.
SLURM_LOGS_DIR          Directory where all the job logs will be stored.
WORKSPACE               Directory where all the processing output will be stored.
MONOCLEANER_MODELS      Directory containing monocleaner models.
PYTHONUSERBASE          Path to the bin directory of Python environment.
PATH                    Add PYTHONUSERBASE to the PATH.
PYTHONPATH              site-packages path to the Python environment.
COLLECTIONS             Associative array with collection names and paths.
```

When using a PIP or Conda environment on LUMI, `PYTHONUSERBASE` AND `PYTHONPATH` are not needed.
Only adding the `bin` directory of the environment to the `PATH` like this
```
export PATH=/project/project_XXXXX/my_dir/my_env/bin:$PATH
```


## Running the pipeline
Running the pipeline is pretty simple.
For each language and for each collection we need to run the following steps.

Do the batching with
```
./00.merge-batching.sh <lang> <collection_name>
```
For example:
```
./00.merge-batching.sh bg wide16
```

After batching is finished (CAUTION! merge-batching job needs to be finished in order to let processing know the number of batches) run the processing with
```
./10.processing.sh <lang> <job_array_index> <collection_name>
```
For example:
```
./10.processing.sh bg all wide17
```

If some jobs fail, they can be run again with
```
./10.processing.sh bg failed wide17
```
or run a subset of all the batches (e.g. because of scheduling restriction does not allow more jobs than the size of the array, or there are other jobs running that are taking up some of the resources allowed) with
```
./10.processing.sh bg 45-118 wide17
```


## Output format
The output format is JSONL, where each line is a valid JSON value and a full document with all its metadata.
For example, the resulting JSON of the text above showed will be something like:
```json
{"id":1, "document_lang":"en", "url":"url1",
    "text":"this is paragraph1\nthis is paragraph2\nthis is paragraph3",
    "lang":["en","en","en"],
    "monocleaner_score":["0.76","0.81","0.80"]
}
{"id":2, "document_lang":"en", "url":"url2",
    "text":"another paragraph\n...",
    ...
```
In each document, each paragraph is concatenated using new-line separators.
`lang` and `monocleaner_score` are lists containing one entry per paragraph, correspongind to the language identified and monocleaner score of each one.

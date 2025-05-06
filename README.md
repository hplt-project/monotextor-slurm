# monotextor-slurm
Set of scripts to deduplicate and annotate monolingual text corpora.
Originally started as a [monotextor](https://github.com/bitextor/monotextor)-like pipeline under Slurm HPCs.


## Pipeline description
### Merge-batching
This pipeline needs as input, directories structured as `$COLLECTION_NAME/$BATCH/{metadata,text,lang}.zst` from [warc2text-runner](https://github.com/hplt-project/warc2text-runner).
The first step will merge for each batch, the three JSONL line-aligned files into a single JSONL, where each document is a JSON object containing the text and all the metadata (`metadata.zst` and `lang.zst`).
Then, for each collection, all the batches will be read sequentially and documents will be placed into separated folders for each language detected and, for each language folder, divide into batches if needed.
On this step, documents with that have prediction probability (`prob`) for the first `lang` field less than 0.5 are discarded.

After the data has been prepared, the actual processing takes place, with near-deduplication and annotation.

### Near-deduplication
Near-deduplication is performed at document level and across all copllections.
For each language, a [MinHash LSH](https://ekzhu.com/datasketch/lsh.html) index is built using a modified version of [gaoya](https://github.com/ZJaume/gaoya/tree/minhash_deduper) library to be able to work with larger scale data.
After an index containing the hashes of all the documents is built, the connected components are computed with [Union-Find](https://en.wikipedia.org/wiki/Disjoint-set_data_structure) algorithm.
Then all the unique documents and one document per cluster are kept.
The input of this step are JSONL files and the output is the same format with near-duplicates removed.

The process is divided into two HyperQueue jobs.
The first one (`./10.index`) reads all the documents, indexes them, builds the connected components and stores in disk the Union-Find vector.
Then, the second one (`./10.dedup`) reads the Union-Find vector and the documents, discarding near-duplicates according to what the vector indicates.

#### Distributed index
For very large languages, a distributed approach has been implemented in the gaoya fork.
This distributed technique runs multiple indexing jobs, where each job stores only one of the MinHash bands.
Thus, all the documents can be indexed in memory.
Otherwise tens of terabytes will be needed if a single process indexes all the bands.

With this approach, each job is computing its own Union-Find vector and storing it in disk.
The dedup step is performed the same way, but instead all the vectors are read and merged at the beginning.

### Robots.txt compliance
To comply with `robots.txt` directives by each web domain, this pipeline includes optional annotation of documents that are not allowed to be crawled.
To do this, the WARCs containing the `robots.txt` files for each collection, have to be provided in the same input directory structure described in [Merge-batching step](#merge-batching).
Afterwards, the `robots.txt` processing can be executed in parallel to the main pipeline and will generate a list of disallowed urls for each collection, that will be used by the annotation step.
The procedure does the following, in one job per collection:
 - Extract all the urls for that collection and create a compressed index with [FST](https://crates.io/crates/fst).
 - Exctract all the documents in JSONL format containing `robots.txt` files from the WARCs.
 - Parse the `robots.txt` files and produce a list allowed and disallowed url patterns for our relevant user agents (`*`, `ia-archiver`, `CCBot`).
 - Use the patterns to query the urls index and generate a list of disallowed patterns.

### Annotation
The annotation step consists of adding multiple metadata fields to each document (using [annotate.py](scripts/annotate.py)):
 - `id`: unique id for the document, derived from the WARC file, url and timestamp (`f`, `u`, `ts` fields).
 - `seg-langs`: segment level language identification. An array of size equal to the number of segments in the document (each segment being delimited by a `\n`). The language identifiaction tool for this step was [heliport](https://github.com/ZJaume/heliport), a fast port of HeLI-OTS trained with the same data as the language identifier for documents.
 - `robots`: robots.txt compliance (if the document has been disallowed for crawling.
 - [monofixer](https://github.com/bitextor/bifixer) to fix encoding issues and remove html entities. This step does not add any metadata field, it just fixes the document text.
 - `pii`: look for PII information with [multilingual-pii-tool](https://github.com/mmanteli/multilingual-PII-tool). In case it any match is found, the field specifies the unicode character offsets for every match.
 - `filter`: if document matches any of the [filtering criteria](#filtering).
 - `doc_scores`: document quality scores with [web-docs-scorer](https://github.com/pablop16n/web-docs-scorer/). An array where the first position is the overall quality score and the rest are the sub-scores used to determine the overall score. All of the scores ranging values from 0 to 10.

The output of this step will produce the same documents as input with the added metadata information.

#### Filters
The process of annotation adds a new metadata field (`filter`) to each document that indicates if the document should be kept or not, and when not, indicate the discarding reason.
Possible values are:
 - `keep`: the document does not match any of the filtering criteria.
 - `adult_ut1`: the url of the document matches one of the domains in [UT1](https://dsi.ut-capitole.fr/blacklists/index_en.php) adult list. To perform matches, full domains are searched in the list. If they don't not match, a second iteration tries search by removing the subdomains.
 - `length_XX`: the text of the document has less than XX characters. Default: 500.
 - `word_avg_X`: the average number of words per segment is less than X. Default: 5.
 - `cha\_avg_X`: the average number of characters per segment is less than X. This is used for Chinese, Japanese and Korean. Default: 10.

### Cleaning
The previous step added lots of metadata for cleaning purposes, but no documents were removed.
To do this, the `30.clean.sh` step needs to be run.
This step will create a new version of the corpus, removing all the documents that do [not meet all of these criteria](30.clean#L26):
 - The `filter` field value is `keep`.
 - The `robots` field value is `allowed`.
 - The overall doccument score (first value of the array: `doc_scores[0]`) is equal or higher than 5.

## Install
To avoid conflicts with the cluster installed software or available modules and be more cluster filesystem friendly, deacreasing dramatically the amount of files needed for the software installation, a Singularity container needs to be built.
The build procedure can be performed in a local machine with these simple steps:
```
# docker build -t monotextor:latest .
# singularity build -F monotextor.sif monotextor.def
# rsync monotextor.sif user@your-cluster:/path/to/monotextor-slurm
```
This requires [Docker](https://docs.docker.com/engine/install/) and [Singularity](https://docs.sylabs.io/guides/3.5/user-guide/quick_start.html) to be installed on the local machine and the cluster has to support Singularity containers execution.

### Set up HyperQueue
For the second and third steps, the pipeline uses [HyperQueue](https://it4innovations.github.io/hyperqueue/stable/installation/) to schedule jobs.
To set it up, follow the HQ installation instructions, placing its binary in any directory of `$PATH`.
Then, it is recommended to start a terminal multiplexer like `screen` or `tmux` and run `hq server start` in a separated window.
After that, the pipeline can start running. HQ is only needed from `10.dedup.sh` and onwards because the merge-batching submits jobs directly to SLURM, so HQ server does not need to be up during that process.

Currently supported version of HQ is 0.22.

## Configure
Copy the `.env.example` to `.env` and edit the variables accordingly.
Needed variables are:
```
SBATCH_ACCOUNT          Project account number.
SLURM_LOGS_DIR          Directory where all the job logs will be stored.
WORKSPACE               Directory where all the processing output will be stored.
FLASH_TMP               Temporary directory for merge-batch parallel step (recommended flash storage partition).
COLLECTIONS             Associative array with collection names and paths.
```

An example of the collections array looks like this:
```bash
INPUT_DIR=/scratch/project_XXXXXX/w2t-runner-output
declare -A COLLECTIONS=(
    ["cc13"]=$INPUT_DIR/CC-MAIN-2013
    ["cc14"]=$INPUT_DIR/CC-MAIN-2014
    ...
    ["cc22"]=$INPUT_DIR/CC-MAIN-2022
    ["cc23"]=$INPUT_DIR/CC-MAIN-2023
    ["wide5"]="$INPUT_DIR/wide00005"
    ["wide6"]="$INPUT_DIR/wide00006"
    ["wide10"]="$INPUT_DIR/wide00010"
    ...
    ["wide15"]="$INPUT_DIR/wide00015"
    ["wide16"]="$INPUT_DIR/wide00016"
    ["wide17"]="$INPUT_DIR/wide00017"
)
```
The pipeline will join multiple collections/crawls if specified as a pattern in the same array entry.
In the latest HPLT there were more than one Common Crawl collections for the same year, so, for example `CC-MAIN-2014-28` and `CC-MAIN-2014-48` are combined as `cc14`.
This allows a more balanced processing during deduplication.

## Running the pipeline
Running the pipeline is pretty simple.
First, for each collection we need to run the merge-batching with
```
./00.merge-batching.sh <collection_name>
```
For example:
```
./00.merge-batching.sh wide16
```
where collection name is one of the keys in the `$COLLECTIONS` array.

After batching is finished for all the collections, run the deduplication with
```
./10.dedup.sh
```
The submission script will create the list of tasks, where each pair of collection-language is a task, then ask for confirmation.
After confirmation, the script will block, showing the progress for all the tasks.
Be aware that this process may take hours or days, so it is recommended to run it in a terminal multiplexer like `tmux` or `screen`, to be able to detach and close the ssh connection to the cluster without killing the process.

**Optional**: while doling the merge batching or dedup steps, robotstxt processing can run in parallel. This step, however does not use HQ, therefore it has to be submitted manually:
```
source .env # load env to have the collections array variable available
for i in ${!COLLECTIONS[@]}; do
    sbatch -J $i-robotstxt --mem-per-cpu 7500 09.robotstxt $i
done
```
It is recommended, as shown in the example above, to use large memory nodes (1TB in this case for the LUMI cluster).

When all the deduplication tasks have finished, the annotation can be eexecuted. For the annotation step, the same logic is applied, just run
```
./20.processing.sh
```
The output will be available at `$WORKSPACE/annotated`.

**Optional**: merge each language into a single directory:
```
source .env
srun -A $SBATCH_ACCOUNT --pty -p small --ntasks 1 --cpus-per-task 128 --mem-per-cpu 1750 -t 24:00:00 bash
```
```
module load parallel
parallel -j64 bash 21.merge-collections ::: `cat langs`
```
That will be available at `$WORKSPACE/collections-merged`.


**Optional**: run the cleaning from the merged collections:
```
./30.clean.sh
```
The output will be available at `$WORKSPACE/cleaned`.

### Tracking logs for HQ tasks
HyperQueue saves logs grouped in files to avoid writing many files.
To take a look at the logs, for example when one or more tasks failed, follow these steps:

During the pipeline step execution, HQ will show the ID of the current job. Use that to list the tasks:
```
hq task list <job_id> --task-status failed
```
Then show the logs from the log directory (note sourcing the env file to have the env variable)
```
source .env
hq output-log $SLURM_LOGS_DIR/hq-20.processing.logs cat <job_id> stderr --task <task_id>
```

If one or more HQ tasks fail during steps 10, 20 or 30, the output logs

## Output format
The output format is JSONL, where each line is a valid JSON value and a full document with all its metadata and text content.
For example, the resulting JSON will be something like:
```json
{"f":"./path/to/80716-00467.warc.gz","o":578687,"s":9202,"rs":102649,
    "u":"https://www.example.com/some_text","c":"text/html","ts":"2021-05-09T10:26:25Z",
    "collection":"wide17",
    "lang":["eng_Latn","fra_Latn","deu_Latn"],"prob":[0.7479,0.076,0.0492],
    "text":"this is paragraph1\nthis is paragraph2\nthis is paragraph3",
    "seg_langs": ["eng_Latn","eng_Latn","eng_Latn"],
    "id":"b8ff3519ba78334d6f63ed20239c42ce",
    "filter":"word_avg_5",
    "pii":[[23,34],[41,45]],
    "doc_scores":[7.7,9.7,10.0,9.8,10.0,9.8,10.0,3.0,0.0],
    "robots":"allowed"}
{"f":"./path/to/80716-00468.warc.gz","o":579437,"s":1100,"rs":44535,
...
    "text":"another paragraph\n...",
...
```
In each document `text` field, each paragraph is concatenated using new-line separators.
The first 7 fields are inherited from `warc2text` HTML extraction from the WARCs, explained [here](https://github.com/bitextor/warc2text/tree/v1.2.0#stdout), with the exception of `p` field which is replaced by `text` and `l` replaced by `lang` and `prob`, which describe the three top identified languages for the document and their prediction probabilities.

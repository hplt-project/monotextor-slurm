# monotextor-slurm
Set of scripts to run [monotextor](https://github.com/bitextor/monotextor)-like pipeline under Slurm HPCs.


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

Current directory pattern expected is `$COLLECTION/$shard/$batch/{plain_text,url}.gz`.

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

### Near-deduplication
Near-deduplication is performed at document level and across all copllections.
For each language, a [MinHash LSH](https://ekzhu.com/datasketch/lsh.html) index is built using a modified version of [gaoya](https://github.com/ZJaume/gaoya/tree/minhash_deduper) library to be able to work with larger scale data.
After an index containing the hashes of all the documents is built, the connected components are computed with [Union-Find](https://en.wikipedia.org/wiki/Disjoint-set_data_structure) algorithm.
Then all the unique documents and one document per cluster are kept.
The input of this step are JSONL files and the output is the same format with near-duplicates removed, with all the collections merged and each language files being splitted again.

The process is divided into two Slurm jobs.
The first one (`./20.index`) reads all the documents, indexes them, builds the connected components and stores in disk the Union-Find vector.
Then, the second one (`./20.dedup`) reads the Union-Find vector and the documents, discarding near-duplicates according to what the vector indicates.

#### Distributed index
For very large languages, a distributed approach has been implemented in the gaoya fork.
This distributed technique runs multiple indexing jobs, where each job stores only one of the MinHash bands.
Thus, all the documents can be indexed in memory.
Otherwise tens of terabytes will be needed if a single process indexes all the bands.

With this approach, each job is computing its own Union-Find vector and storing it in disk.
The dedup step is performed the same way, but instead all the vectors are read and merged at the beginning.

### Filtering
The process of cleaning adds a new metadata field (`"filter"`) to each document that indicates if the document should be discarded or not and when not, the discarding reason.
Possible values are:
 - `keep`: the document does not match any of the filtering criteria.
 - `adult_ut1`: the url of the document matches one of the domains in UT1 adult list. To perform matches, full domains are searched in the list. If they don't not match, a second iteration tries search by removing the subdomains.
 - `length_XX`: the text of the document has less than XX characters. Default: 200.
 - `lang_ratio_XX`: the ratio of languages by segment that match the document language is less than XX. Default: 0.2 (at least 20% of the segment languages in a document are the same as the document language).
 - `word_avg_X`: the average number of words per segment is less than X. Default: 5.
 - `cha\_avg_X`: the average number of characters per segment is less than X. This is used for Chinese, Japanese and Korean. Default: 10.

There are languages considered exceptions for the language ratio rule and it is disabled.
This is mainly because some languages either have poor language identification at segment level or the the majority of documents have a very high portion of boilerplate and/or English.
Sometimes both cases.
Therefore language ratio rule ends up being too aggressive.
These language exceptions are:
 - Afrikaans, Swahili, Somali and Tagalog for the reasons explained above.
 - Uzbek segment level language identification is tagging all the Cyrillic as other languages.
 - Malay and Indonesian tend to mix up with each other.

## Install
Install requirements inside your virtual environment.
```
pip install -r requirements.txt
```

Install Rust utils:
```
cargo install --root path/to/venv --path utils/
```
root directory to be installed can be any directory containing a `bin` directory and make sure it is in `PATH`.
For example another path could be `$HOME/.local` and in that case, add `$HOME/.local/bin` to `PATH` in the configuration below.

## Configure
Copy the `.env.example` to `.env` and edit the variables accordingly.
Needed variables are:
```
SBATCH_ACCOUNT          Project account number.
SLURM_LOGS_DIR          Directory where all the job logs will be stored.
WORKSPACE               Directory where all the processing output will be stored.
MONOCLEANER_MODELS      Directory containing monocleaner models.
FLASH_TMP               Temporary directory for merge-batch parallel step (recommended flash storage partition).
PYTHONUSERBASE          Path to the bin directory of Python environment and Rust utils.
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

To run deduplication, simply run:
```
./20.dedup.sh bg
```

For very large languages (probably Chinese and English), distributed approach is needed. To do that run:
```
./20.dedup.sh en dist
```
This will run for the index step a job array, where each job indexes and stores only one MinHash band.

Note that the [Slurm parameters](https://github.com/hplt-project/monotextor-slurm/blob/0c66e74db65acc489b2cb7b712d558a0cebe4f42/20.index#L8) for the indexing step will need to be adjusted accordingly.
For the HPLT v1.1, 128 cores an 7000MB per core were used, in order to use 1TB nodes in LUMI.

### Retrying
The proecessing script also takes care of waiting and retrying submit if a submission fails due to `AssocMaxSubmitJobLimit` error.
In that case the script proces will wait indefinetly until it's able to submit all the remaining jobs.
Also, in case of the job array index being larger than the limit (currently 120), it will submit them in groups of 60.


## Output format
The output format is JSONL, where each line is a valid JSON value and a full document with all its metadata.
For example, the resulting JSON will be something like:
```json
{"id":1, "document_lang":"en", 
    "scores":[0.76,0.76,0.76],
    "langs":["en","en","en"],
    "text":"this is paragraph1\nthis is paragraph2\nthis is paragraph3",
    "url":"url1", "collection":"collection-1" 
}
{"id":2, "document_lang":"en",
    "scores":[0.65,...],
    "langs":["en",...],
    "text":"another paragraph\n...",
...
```
In each document, each paragraph is concatenated using new-line separators.
`langs` and `scores` are lists containing one entry per paragraph, corresponding to the language identified and monocleaner score of each one.

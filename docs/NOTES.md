# Notes about the downloader

Among the 31589 datasets reported in the datasets.json file, 26442 datasets have been completely downloaded.
- 4866 were found to be unavailable (HTTP error code 403/404/410/502)
- 281 datasets were found to be incomplete (some files of the dataset couldn't be downloaded)
- The download of about 70 files failed because of rejected connections from the servers due to multiple GET requests made.

Of the 28499 files I was able to download (about 41GB), about 260 turn out to be not directly usable, as the file type is not between ['rdf', 'ttl', 'owl', 'n3', 'nt', 'jsonld'].
Most of these files turn out to be in compressed format (zipper or tar) ot HTML files

Many downloads are though API endpoints and a version for the file has not been provided, so it is possible that the 
files you obtain have different content with respect to the ones I'm using to develop this system.

The new implementation of the `dataset_downloader.py` is ready to be parallelized, but doing so can 
result in the blacklisting IP of the downloading machine.

Many entries in `datasets.json` present duplicate links, it is necessary to remove duplicate links

___

## Notes about the data extractor

RDFLIB is used to issue some queries to the downloaded datasets.
The first implementation was a single process (and single thread) and while increasing the size of the 
collection during test I realized that this solution was not feasible due to time constraint 
(it required days of uninterrupted computation).

For this reason, `parallel_dataset_extractor.py` has been created.
Taking advantage of Python `multiprocess` library it has been possible to parallelize the code.
This approach increased CPU usage (now it scales on all cores) and decreases the running time 
almost by a 3 factor.

Benchmark on a toy collection composed of 130 datasets
|File   	                  | Time (ms) |
|-----------------------------|-----------|
|dataset_extractor.py         |271.156ms  |
|parallel_dataset_extractor.py|109.784ms  |

A dataset is complete if all files have been downloaded and no file has thrown any error while mining it,
so in the `metadata.json` all the conditions are satisfied:
- `used_files` is greater then zero
- `failed_download_urls` is an empty list
- `unused_files` is an empty list

### RDFLib and RAM usage

In order to tackle the memory usage of RDFLib I removed from my collection all the files which were more than 100MB in size.
Assuming `datasets` is the folder from the current working directory in which datasets are stored, by running you can find all the datasets which size is more then 100MB

```sh
find datasets -type f -size +100M -exec ls -alh {} \; | sort -hr -k5
```
# Stats

In this folder (and its sub folders) I will report some data related to the downloaded collection.

- The folder [raw](./raw/) contains information about the raw collection, without any modification after downloading.  
It contains:
    - Execution of [stats](./stats.ipynb) with output, saved as [HTML file](./raw/stats.html)
    - [tree.txt](./raw/tree.txt) dump of the downloaded directory structure

## Check diff in tree structure after manual processing
Command used: `diff -y --suppress-common-lines raw/tree.txt mods/tree.txt`
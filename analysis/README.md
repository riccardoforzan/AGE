# Stats

In this folder (and its sub folders) I will report some data related to the downloaded collection.

- The folder [raw](./raw/) contains information about the raw collection, without any modification after downloading.  
It contains:
    - [Executed jupyter notebook](./raw/analysis.ipynb) that gives insights on the processed collection
    - [tree.txt](./raw/tree.txt) dump of the downloaded directory structure

- The folder [mods](./mods/) contains the modification performed on the raw collection
    - [Executed jupyter notebook](./mods/mods.ipynb) used to obtain the modified collection starting from the raw one
    - [tree.txt](./mods/tree.txt) dump of the downloaded directory structure

## Check diff in tree structure after manual processing
Command used: `diff -y --suppress-common-lines raw/tree.txt mods/tree.txt`
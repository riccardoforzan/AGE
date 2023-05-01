"""
This scripts generates a folder with a subset of the collection.
This script has been used to restrict the scope of the execution while setting up the pipeline.

It takes 3 command line arguments:
1) The number of datasets to pick at random
2) The input folder that contains the datasets
3) The path of the output folder
"""

import os
import argparse
import shutil
import random

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "datasets", type=int, help="Number of datasets to pick at random"
    )
    parser.add_argument("source", type=str, help="Folder in which datasets are stored")
    parser.add_argument(
        "target", type=str, help="Folder in which datasets will be stored"
    )
    args = parser.parse_args()

    pick_datasets = args.datasets
    source_folder = args.source
    target_folder = args.target

    datasets = os.listdir(source_folder)
    random.shuffle(datasets)

    i = 0
    while i < pick_datasets:
        dataset_id = datasets[i]

        source_path = os.path.join(f"{source_folder}/{dataset_id}")
        target_path = os.path.join(f"{target_folder}/{dataset_id}")

        if os.path.isdir(source_path):
            shutil.copytree(source_path, target_path, dirs_exist_ok=True)

        i += 1

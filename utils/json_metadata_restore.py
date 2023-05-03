"""
For each dataset folder restores `metadata.json` deleting what has been appended by `extract.py`
"""

import os
import json
import argparse


def restore_dataset(dataset_path: str):
    metadata_file = f"{dataset_path}/metadata.json"

    # restore `metadata.json` file
    with open(metadata_file, "r+") as mf:
        data = json.load(mf, strict=False)

        og = dict()

        og["dataset_id"] = data["dataset_id"]
        og["title"] = data["title"]
        og["description"] = data["description"]
        og["author"] = data["author"]
        og["tags"] = data["tags"]

        og["downloaded_urls"] = data["downloaded_urls"]
        og["failed_download_urls"] = data["failed_download_urls"]

        content = json.dumps(og, ensure_ascii=False, indent=4)

        # back to the head of the file, truncate the content and serialize default
        mf.seek(0)
        mf.truncate(0)
        mf.write(content)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("folder", type=str, help="Folder that contains the datasets")
    args = parser.parse_args()

    datasets = os.listdir(args.folder)

    for d in datasets:
        print(f"Processing dataset {d}")
        path = f"{args.folder}/{d}"
        restore_dataset(dataset_path=path)

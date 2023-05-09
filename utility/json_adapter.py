"""
For each dataset folder, creates a new file `metadata-cc.json`
that uses the default camelCase notation for JSON files
instead the snake_case used in the `metadata.json`
"""

import os
import json
import argparse


def create_camel_case_json_file(dataset_path: str):
    metadata_file = f"{dataset_path}/metadata.json"
    camel_case_json = f"{dataset_path}/metadata-cc.json"

    if os.path.isfile(camel_case_json):
        raise Exception(f"{camel_case_json} already exists")

    with open(metadata_file, "r") as og, open(camel_case_json, "w+") as ccj:
        data = json.load(og, strict=False)

        ccf = dict()
        ccf["id"] = data["dataset_id"]
        ccf["title"] = data["title"]
        ccf["description"] = data["description"]
        ccf["author"] = data["author"]
        ccf["tags"] = data["tags"]

        l = list()
        for entry in data["downloaded_urls"]:
            l.append({"url": entry["url"], "file": entry["file_name"]})

        ccf["downloadedURLs"] = l

        ccf["failedURLs"] = data["failed_download_urls"]

        ccf["classes"] = data["classes"]
        ccf["literals"] = data["literals"]
        ccf["entities"] = data["entities"]
        ccf["properties"] = data["properties"]
        ccf["connections"] = data["connections"]

        ccf["connectedVertices"] = data["connected_vertices"]
        ccf["averageLiteralsPerVertex"] = data["average_literals_per_vertex"]
        ccf["usedFiles"] = data["used_files"]
        ccf["unusedFiles"] = data["unused_files"]

        content = json.dumps(ccf, ensure_ascii=False, indent=4)
        ccj.write(content)


def remove_generated_json_file(dataset_path: str):
    camel_case_json = f"{dataset_path}/metadata-jf.json"
    if os.path.isfile(camel_case_json):
        os.remove(camel_case_json)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("folder", type=str, help="Folder that contains the datasets")

    parser.add_argument(
        "--generate",
        action=argparse.BooleanOptionalAction,
        help="Generates the file `metadata-cc.json` for each dataset",
    )

    parser.add_argument(
        "--remove-generated",
        action=argparse.BooleanOptionalAction,
        help="Removes the file `metadata-cc.json` from each dataset folder",
    )

    args = parser.parse_args()

    generate = args.generate == True
    remove_generated = args.remove_generated == True

    datasets = os.listdir(args.folder)

    if (not generate and not remove_generated) or (generate and remove_generated):
        print("Select exactly ONE option, use -h for help")

    if generate:
        for d in datasets:
            print(f"Processing dataset {d}")
            path = f"{args.folder}/{d}"
            create_camel_case_json_file(dataset_path=path)

    if remove_generated:
        for d in datasets:
            print(f"Processing dataset {d}")
            path = f"{args.folder}/{d}"
            remove_generated_json_file(path)

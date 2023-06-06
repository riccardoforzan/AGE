"""
This script tries to download all the files reported in the ACORDAR test collection.
It requires the path to the file `datasets.json` that you can find at https://github.com/nju-websoft/ACORDAR/blob/main/Data/datasets.json

If while downloading something goes wrong you can resume the execution using the option `--start-from` and specifying the last dataset downloaded
"""

import os
import json
import time
import logging
import pathlib
import requests
import argparse
from pathlib import Path
from slugify import slugify
from rich.progress import Progress

# Accepted file suffixes
RDF_SUFFIXES = ["rdf", "ttl", "owl", "n3", "nt", "jsonld", "nq", "trig", "trix"]


def clean_string(s: str) -> str:
    return " ".join(s.split()).encode("unicode_escape").decode("unicode_escape")


def file_name(url: str, directory_path: str) -> str:
    file_name = None

    if url.endswith("/"):
        url = url[:-1]

    # try to retrieve the resource name and extension from the URL
    target_resource = url.split("/")[-1]

    resource_name = target_resource.split(".")[0]
    resource_extension = target_resource.split(".")[-1]

    # examine the file and its extension
    file_extension = None
    for ext in RDF_SUFFIXES:
        if ext in resource_extension:
            file_extension = ext

    if file_extension is not None:
        slug = slugify(resource_name)
        file_name = f"{slug}.{file_extension}"

    # if the file does not have a valid extension then use the resource name in the URL
    if file_extension is None:
        file_name = ".".join(
            list(map(lambda x: slugify(x), target_resource.split(".")))
        )

    # check that the directory does not already contains a file with the same name
    if os.path.isdir(directory_path):
        files_in_directory = os.listdir(directory_path)

        # if a file with the same name has been found generate a timestamp and append file name
        if file_name in files_in_directory:
            time_str = time.strftime("%Y_%m_%d-%I_%M_%S")
            file_name = f"{time_str}-{file_name}"

    return file_name


def download_from_url(url: str, folder: str, file_name: str) -> bool:
    """Downloads the resource from the given URL

    Args:
        url (str): URL that contains the item to be downloaded
        folder (str): target folder in which the downloaded item will be saved
        file_name (str): name of the downloaded file

    Returns:
        bool: True if the download has been completed without errors, otherwise an exception is thrown
    """

    response = requests.get(url, stream=True)
    response.raise_for_status()

    download_path = folder + f"/{file_name}"
    Path(folder).mkdir(parents=True, exist_ok=True)

    # Create a progress bar to track progress while downloading
    total_size_in_bytes = int(response.headers.get("content-length", 0))

    with (
        Progress(expand=True) as progress_bar,
        open(download_path, "wb") as target,
    ):
        print(f"Downloading {url}")
        task = progress_bar.add_task(
            f"[green]Downloading...", total=total_size_in_bytes
        )

        for data in response.iter_content():
            progress_bar.update(task, advance=len(data))
            target.write(data)

    return True


def process_dataset(index: int, entry: dict, folder: str):
    global log

    dataset_id = entry["dataset_id"]

    # remove duplicate download links
    dataset_urls = list()
    for url in entry["download"]:
        if url not in dataset_urls:
            dataset_urls.append(url)

    download_folder = f"{folder}/{dataset_id}"

    # download attached files
    downloaded_entries = list()
    failed_urls = list()
    for url in dataset_urls:
        try:
            downloaded_file_name = file_name(url, download_folder)
            download_from_url(url, download_folder, downloaded_file_name)

            # save the downloaded file into the downloaded entry
            downloaded_entries.append({"url": url, "name": downloaded_file_name})

        except Exception as err:
            failed_urls.append(url)
            log.warning(
                f"""
                ERROR while downloading [Dataset index: {index}] [Dataset ID: {dataset_id}] url: {url}
                Details: {str(err)}
                """
            )

    # get metadata from the entry
    title = entry.get("title", "")
    description = entry.get("description", "")
    author = entry.get("author", "")
    tags = entry.get("tags", "")

    # serialize metadata and download information
    data = dict()
    data["id"] = dataset_id
    data["title"] = clean_string(title)
    data["description"] = clean_string(description)
    data["author"] = clean_string(author)
    data["tags"] = clean_string(tags)
    data["downloadedURLs"] = downloaded_entries
    data["failedURLs"] = failed_urls

    # prepare the JSON representation of the metadata
    content = json.dumps(data, ensure_ascii=False, indent=4)

    # file in which the data will be stored
    outfile = f"{download_folder}/metadata.json"

    # check if data file already exists
    data_file_already_exists = os.path.isfile(outfile)

    if data_file_already_exists:
        log.warning(f"File {outfile} already exists")

    output_file = pathlib.Path(outfile)
    output_file.parent.mkdir(exist_ok=True, parents=True)
    output_file.write_text(content)

    pass


if __name__ == "__main__":
    # Get from the arguments the file to read
    parser = argparse.ArgumentParser()
    parser.add_argument("file", type=str, help="File that contains the dataset (JSON)")
    parser.add_argument(
        "folder", type=str, help="Path of the folder in which files will be downloaded"
    )
    parser.add_argument(
        "--start-from",
        type=int,
        help="Start downloading from the given index (included)",
    )
    args = parser.parse_args()

    # read JSON file provided as input
    f = open(args.file)
    data = json.load(f, strict=False)
    f.close()

    # download folder
    download_folder = args.folder

    # check if resume download index has been specified
    resume_from = None
    if args.start_from is not None:
        resume_from = args.start_from

    # Used as index for dataset iterations
    index = 0

    logging.basicConfig(
        filename="download.log",
        filemode="w",
        format="%(asctime)-15s %(levelname)-8s %(message)s",
    )
    log = logging.getLogger("datasets-downloader")

    # start processing each dataset
    datasets = sorted(data["datasets"], key=lambda d: int(d["dataset_id"]))

    if not os.path.isdir(download_folder):
        os.mkdir(download_folder)

    for entry in datasets:
        dataset_id = entry["dataset_id"]

        # Resume execution if needed
        if resume_from is not None and index < resume_from:
            print(f"Skipping dataset with ID {dataset_id} - [INDEX: {index}]")
            index += 1
            continue

        # process a dataset
        print(
            f"Processing dataset [ID: {dataset_id}] [INDEX: {index}] downloads: {len(entry['download'])}"
        )

        process_dataset(index, entry, download_folder)
        index += 1

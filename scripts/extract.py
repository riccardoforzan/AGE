"""
Extracts data from all the files with valid suffixes (reported in RDF_SUFFIXES).
It appends extracted data in the `metadata.json` file that is contained inside every dataset folder.
"""

import os
import json
import logging
import pathlib
import argparse
import statistics
from rdflib import Graph
from functools import partial
from rich.progress import Progress
from multiprocessing import Pool, cpu_count

RDF_SUFFIXES = ["rdf", "ttl", "owl", "n3", "nt", "jsonld", "nq", "trig", "trix"]

SIZE_LIMIT = 100 * 1024 * 1024  # 100 MB

logging.basicConfig(
    level=logging.INFO,
    filename="extract.log",
    filemode="w",
    format="%(asctime)-15s %(levelname)-8s %(message)s",
)

log = logging.getLogger()


def clean_string(s: str) -> str:
    return " ".join(s.split()).encode("unicode_escape").decode("unicode_escape")


def get_literals(graph) -> list:
    q = """
    SELECT ?literal { 
        ?s ?p ?literal 
        FILTER isLiteral(?literal)
    }
    """
    match = graph.query(q)

    literals = list()

    for item in match:
        label = clean_string(str(item[0]))
        literals.append(label)

    return literals


def get_classes(graph) -> list:
    q = """
    SELECT ?class
    WHERE {
        ?s a ?class .
    }
    """
    match = graph.query(q)

    classes = list()

    for item in match:
        label = clean_string(str(item[0]))
        classes.append(label)

    return classes


def get_entities(graph) -> list:
    q = """
    SELECT ?s
    WHERE {
        ?s a ?class .
    }
    """
    match = graph.query(q)

    entities = list()

    for item in match:
        name = clean_string(str(item[0]))
        entities.append(name)

    return entities


def get_properties(graph) -> list:
    q = """
    SELECT ?p
    WHERE {
        ?s ?p ?o .
    }
    """
    match = graph.query(q)

    properties = list()

    for item in match:
        label = clean_string(str(item[0]))
        properties.append(label)

    return properties


def get_number_of_connections(graph) -> int:
    q = """SELECT (count(*) as ?count) 
        WHERE {
            ?s ?p ?o.
            FILTER(!IsLiteral(?s))
            FILTER(!IsLiteral(?o))
        }
    """
    match = graph.query(q)

    count = 0

    for item in match:
        label = str(item[0])
        count = int(label)

    return count


def get_number_of_connected_vertices(graph) -> int:
    q = """SELECT (count(DISTINCT ?vertex) as ?count) WHERE {
            { 
                ?vertex ?p [] 
            }
            UNION
            { 
                [] ?p ?vertex 
                FILTER(!IsLiteral(?vertex))
            }
        }
    """
    match = graph.query(q)

    count = 0

    for item in match:
        label = str(item[0])
        count = int(label)

    return count


def get_average_of_literals_per_vertex(graph) -> float:
    q = """
    SELECT ?s (count(?o) as ?count)
    WHERE{
        ?s ?p ?o. 
        FILTER(!IsLiteral(?s))
        FILTER(IsLiteral(?o))
    }
    GROUP BY ?s
    """
    match = graph.query(q)

    vertices = 0
    literals = 0

    for item in match:
        vertices += 1
        literals += int(item[1])

    avg = literals / vertices
    return round(avg, 3)


# processing functions


def extract_data_from_file(file_path: str) -> dict:
    file_suffix = pathlib.Path(file_path).suffix.replace(".", "")

    # if the file does not have a suffix that matches the one allowed report it
    file_extension = None
    for ext in RDF_SUFFIXES:
        if ext in file_suffix:
            file_extension = ext

    if file_extension is None:
        raise ValueError(f"File {file_path} does not match any of allowed extensions")

    # load data into graph
    graph = Graph()
    graph.parse(file_path)

    data = dict()
    data["classes"] = get_classes(graph)
    data["literals"] = get_literals(graph)
    data["entities"] = get_entities(graph)
    data["properties"] = get_properties(graph)
    data["connections"] = get_number_of_connections(graph)
    data["connected_vertices"] = get_number_of_connected_vertices(graph)
    data["average_literals_per_vertex"] = get_average_of_literals_per_vertex(graph)

    return data


def process_dataset(
    dataset_folder: str,
    with_size_limit: bool,
    skip_already_processed: bool,
):
    # check that the given path is a folder
    if not os.path.isdir(dataset_folder):
        log.error(f"Path {dataset_folder} is not a folder")

    # check if metadata file exists
    metadata_file = f"{dataset_folder}/metadata.json"

    data_file_already_exists = os.path.isfile(metadata_file)
    if not data_file_already_exists:
        log.error(f"File {metadata_file} does not exists")

    # read metadata.json object from file
    with open(metadata_file, "r+") as f:
        # read JSON object from the file
        metadata = json.load(f, strict=False)

        # check if the dataset has been already processed
        already_processed = "used_files" in metadata

        # if the dataset has already been processed and skip already processed, then return
        if already_processed and skip_already_processed:
            log.info(f"Skipping already processed {dataset_folder}")
            return None

        # get a list of all the files inside the directory
        files_in_directory = os.listdir(dataset_folder)

        # remove from the files to analyze `metadata.json`
        files_in_directory.remove("metadata.json")

        usable_files = list()  # files that potentially can be mined
        used_files = list()  # files successfully mined
        unused_files = list()  # files not mined (format or parsing issues)

        # filter out the files from the usable ones
        for file in files_in_directory:
            ext = file.split(".")[-1]

            file_path = f"{dataset_folder}/{file}"
            file_size = os.path.getsize(file_path)

            # if the file size is greater then the file limit discard it
            if with_size_limit and file_size > SIZE_LIMIT:
                unused_files.append(file)
                log.warning(
                    f"{dataset_folder}/{file} size is greater than the size limit, this dataset has been marked as not usable"
                )
                continue

            # check if the extension of the file is one among the ones that can be parsed
            if ext in RDF_SUFFIXES:
                usable_files.append(file)

            else:
                unused_files.append(file)
                log.warning(f"{dataset_folder}/{file} does not have a valid extension")

        # start extracting from the files inside the folder
        classes = list()
        literals = list()
        entities = list()
        properties = list()
        connections = list()
        connected_vertices = list()
        average_literals_per_vertex = list()

        # extract data from the files inside the directory of the dataset
        for file in usable_files:
            try:
                file_path = f"{dataset_folder}/{file}"
                data = extract_data_from_file(file_path)

                # merge extracted data
                classes.extend(data["classes"])
                literals.extend(data["literals"])
                entities.extend(data["entities"])
                properties.extend(data["properties"])
                connections.append(data["connections"])
                connected_vertices.append(data["connected_vertices"])
                average_literals_per_vertex.append(data["average_literals_per_vertex"])

                # register the used file
                used_files.append(file)

            except Exception as e:
                unused_files.append(file)
                log.error(f"Exception occurred while processing {file_path}: {str(e)}")
            finally:
                continue

        # merge together all the data for the dataset
        metadata["classes"] = classes
        metadata["literals"] = literals
        metadata["entities"] = entities
        metadata["properties"] = properties
        metadata["connections"] = sum(connections)
        metadata["connected_vertices"] = sum(connected_vertices)
        metadata["average_literals_per_vertex"] = (
            statistics.fmean(average_literals_per_vertex)
            if len(average_literals_per_vertex) > 0
            else 0
        )

        # register used and unused files
        metadata["used_files"] = used_files
        metadata["unused_files"] = unused_files

        # print out a JSON file containing all the data
        content = json.dumps(metadata, ensure_ascii=False, indent=4)

        # write to `metadata.json` with the new extracted data
        f.seek(0)  # return to the top of the file
        f.truncate(0)
        f.write(content)

        log.info(f"Processed {dataset_folder}")

    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("folder", type=str, help="Folder in which datasets are stored")
    parser.add_argument(
        "--without-size-limit",
        action=argparse.BooleanOptionalAction,
        help="Enables the processing of files which size is more than 100MB",
    )
    parser.add_argument(
        "--skip-processed",
        action=argparse.BooleanOptionalAction,
        help="Datasets folders that already contain a file `metadata.json` will be skip",
    )
    args = parser.parse_args()

    dataset_folder = args.folder
    skip_already_processed = args.skip_processed == True
    without_size_limit = args.without_size_limit == True

    if not without_size_limit:
        print(f"Size limit set to {SIZE_LIMIT} byte")

    # map all the datasets to their relative paths
    datasets = list(
        map(lambda did: f"{dataset_folder}/{did}", os.listdir(dataset_folder))
    )

    # parametrize the function call that is going to be executed in the pool
    parametrized_function_call = partial(
        process_dataset,
        skip_already_processed=skip_already_processed,
        with_size_limit=not without_size_limit,
    )

    # create the pool and assign jobs to the pool
    pool_size = cpu_count() - 1

    with Pool(pool_size) as p, Progress() as progress:
        task = progress.add_task("[green]Processing...", total=len(datasets))

        for _ in p.imap_unordered(parametrized_function_call, datasets):
            progress.update(task, advance=1)

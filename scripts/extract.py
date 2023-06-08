"""
Extracts data from all the files with valid suffixes (reported in RDF_SUFFIXES).
It appends extracted data in the `metadata.json` file that is contained inside every dataset folder.
"""

import os
import json
import logging
import pathlib
import argparse
from rdflib import Graph
from slugify import slugify
from datetime import datetime
from functools import partial
from rich.progress import Progress
from multiprocessing import Pool, cpu_count
from rdflib.extras.external_graph_libs import rdflib_to_networkx_graph

RDF_SUFFIXES = ["rdf", "ttl", "owl", "n3", "nt", "jsonld", "nq", "trig", "trix"]

# Files larger than this size are not going to be processed through RDFLib
SIZE_LIMIT = 200 * 1024 * 1024  # 200 MB


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


def extract_data_from_file(dataset_folder: str, dataset: str, file: str) -> dict:
    base_dataset_path = f"{dataset_folder}/{dataset}"
    file_path = f"{base_dataset_path}/{file}"

    # if the file does not have a suffix that matches the one allowed report it
    file_suffix = pathlib.Path(file_path).suffix.replace(".", "")

    file_extension = None

    for ext in RDF_SUFFIXES:
        if ext in file_suffix:
            file_extension = ext

    if file_extension is None:
        raise ValueError(f"File {file_path} does not match any of allowed extensions")

    # load data into graph
    graph = Graph()
    graph.parse(file_path)

    # create output file names
    base_name = f"{datetime.now().strftime('%Y%m%d-%H%M%S')}-{slugify(file)}"

    # write entities to file
    entities_file = f"{base_name}-entities.txt"
    with open(f"{base_dataset_path}/{entities_file}", "w+") as e_out:
        for e in get_entities(graph):
            e_out.write(f"{e}\n")

    # write properties to file
    properties_file = f"{base_name}-properties.txt"
    with open(f"{base_dataset_path}/{properties_file}", "w+") as p_out:
        for p in get_properties(graph):
            p_out.write(f"{p}\n")

    # write literals to file
    literals_file = f"{base_name}-literals.txt"
    with open(f"{base_dataset_path}/{literals_file}", "w+") as l_out:
        for l in get_literals(graph):
            l_out.write(f"{l}\n")

    # write classes to file
    classes_file = f"{base_name}-classes.txt"
    with open(f"{base_dataset_path}/{classes_file}", "w+") as c_out:
        for c in get_classes(graph):
            c_out.write(f"{c}\n")

    # create representation for the parsed dataset
    entry = {
        "file": file,
        "size": os.path.getsize(file_path),
        "classesFile": classes_file,
        "literalsFile": literals_file,
        "entitiesFile": entities_file,
        "propertiesFile": properties_file,
        "connections": get_number_of_connections(graph),
        "connectedVertices": get_number_of_connected_vertices(graph),
        "averageLiteralsPerVertex": get_average_of_literals_per_vertex(graph),
        "extractedWith": "RDFLib",
    }

    return entry


def process_dataset(
    with_size_limit: bool,
    datasets_folder: str,
    dataset: str,
):
    global log

    # path of where dataset files are stored
    dataset_folder = f"{datasets_folder}/{dataset}"

    # check that the given path is a folder
    if not os.path.isdir(dataset_folder):
        log.error(f"Path {dataset_folder} is not a folder")
        return

    # check if metadata file exists
    metadata_file = f"{dataset_folder}/metadata.json"

    data_file_exists = os.path.isfile(metadata_file)
    if not data_file_exists:
        log.error(f"File {metadata_file} does not exists")
        return

    # read metadata.json object from file
    with open(metadata_file, "r+") as f:
        metadata = json.load(f, strict=False)

        # delete file created by previous processing
        if "extracted" in metadata.keys():
            keys = ["classesFile", "literalsFile", "entitiesFile", "propertiesFile"]
            for item in metadata["extracted"]:
                for k in keys:
                    if k in item.keys():
                        ftdp = f"{dataset_folder}/{item[k]}"
                        if os.path.exists(ftdp):
                            os.remove(ftdp)


        # get a list of all the files inside the directory
        files_in_directory = os.listdir(dataset_folder)

        files_in_directory.remove("metadata.json")

        usable_files = list()  # files that potentially can be used
        unused_files = list()  # files not used (format or parsing issues)

        # filter usable files
        for file in files_in_directory:
            ext = file.split(".")[-1]

            file_path = f"{dataset_folder}/{file}"
            file_size = os.path.getsize(file_path)

            # check if the extension of the file is one among the ones that can be parsed
            if ext not in RDF_SUFFIXES:
                unused_files.append({"file": file, "size": file_size})
                log.warning(f"{dataset_folder}/{file} does not have a valid extension")
                continue

            # if the file size is greater then the file limit then skip it
            if with_size_limit and file_size > SIZE_LIMIT:
                unused_files.append({"file": file, "size": file_size})
                log.warning(f"{dataset_folder}/{file} sizd is over the limit")
                continue

            # if the file survived all the filtering above then mark it as usable
            usable_files.append(file)

        # start extracting from the files inside the folder
        extracted = list()

        # extract data from the files inside the directory of the dataset
        for file in usable_files:
            file_path = f"{dataset_folder}/{file}"
            file_size = os.path.getsize(file_path)

            try:
                # represent the data extracted from this file
                representation = extract_data_from_file(datasets_folder, dataset, file)
                extracted.append(representation)

            except Exception as e:
                unused_files.append({"file": file, "size": file_size})
                log.error(f"Exception occurred while processing {file_path}: {str(e)}")

            finally:
                continue

        # save extracted data
        metadata["extracted"] = extracted
        metadata["unusedFiles"] = unused_files

        # print out a JSON file containing all the data
        content = json.dumps(metadata, ensure_ascii=False, indent=4)

        # write to `metadata.json` with the new extracted data
        f.seek(0)  # return to the top of the file
        f.truncate(0)
        f.write(content)

        log.info(f"Processed {dataset_folder}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("folder", type=str, help="Folder in which datasets are stored")
    parser.add_argument(
        "--without-size-limit",
        default=False,
        action=argparse.BooleanOptionalAction,
        help="Enables the processing of files which size is more than 200MB",
    )

    args = parser.parse_args()
    datasets_folder = args.folder
    without_size_limit = args.without_size_limit == True

    logging.basicConfig(
        level=logging.INFO,
        filename="extract.log",
        filemode="w",
        format="%(asctime)-15s %(levelname)-8s %(message)s",
    )

    log = logging.getLogger()

    if not without_size_limit:
        print(f"Size limit set to {SIZE_LIMIT} byte")

    datasets = sorted(os.listdir(datasets_folder))

    # parametrize the function call that is going to be executed in the pool
    parametrized_function_call = partial(
        process_dataset, not without_size_limit, datasets_folder
    )

    # create the pool and assign jobs to the pool
    pool_size = cpu_count() - 1

    with Pool(pool_size) as p, Progress(expand=True) as progress:
        task = progress.add_task("[green]Processing...", total=len(datasets))

        for _ in p.imap_unordered(parametrized_function_call, datasets):
            progress.update(task, advance=1)

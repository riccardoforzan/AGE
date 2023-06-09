import os
import json
import queue
import logging
import pathlib
import argparse
import lightrdf
from slugify import slugify
from statistics import mean
from datetime import datetime
from collections import defaultdict

RDF_SUFFIXES = ["rdf", "ttl", "owl", "n3", "nt", "jsonld", "nq", "trig", "trix"]

SIZE_LIMIT = 200 * 1024 * 1024  # 200 MB


def clean_string(s: str) -> str:
    return " ".join(s.split()).encode("unicode_escape").decode("unicode_escape")


def is_literal(node: str) -> bool:
    return node.startswith('"') and node.endswith('"')


def process_triples(
    file_path: str,
    entities_file: str,
    properties_file: str,
    literals_file: str,
    classes_file: str,
) -> dict:
    with (
        open(entities_file, "w+") as e_out,
        open(properties_file, "w+") as p_out,
        open(literals_file, "w+") as l_out,
        open(classes_file, "w+") as c_out,
    ):
        doc = lightrdf.RDFDocument(file_path)

        number_of_connections = 0
        vertices_count_literals = defaultdict(int)

        for triple in doc.search_triples(None, None, None):
            sub = triple[0]
            prop = triple[1]
            obj = triple[2]

            print(clean_string(sub), file=e_out)

            if "type" in prop.lower() or "a" == prop.lower():
                print(obj, file=c_out)
                continue

            print(clean_string(prop), file=p_out)

            is_obj_literal = is_literal(obj)

            if is_obj_literal:
                obj_repr = clean_string(eval(obj))
                if len(obj_repr) > 0:
                    print(obj_repr, file=l_out)

            if not is_obj_literal:
                print(clean_string(obj), file=e_out)
                number_of_connections += 1

            # count literals for each vertex
            if sub not in vertices_count_literals:
                vertices_count_literals[sub] = 0

            vertices_count_literals[sub] += 1

        connected_vertices = len(vertices_count_literals.keys())
        average_literals_per_vertex = mean(vertices_count_literals.values())

        return {
            "connections": number_of_connections,
            "connected_vertices": connected_vertices,
            "average_literals_per_vertex": average_literals_per_vertex,
        }


def process_file(datasets_folder: str, dataset: str, file: str):
    # file to be analyzed
    base_dataset_path = f"{datasets_folder}/{dataset}"
    file_path = f"{base_dataset_path}/{file}"
    metadata_file_path = f"{base_dataset_path}/metadata.json"

    # create output file names
    base_name = f"{datetime.now().strftime('%Y%m%d-%H%M%S')}-{slugify(file)}"
    entities_file = f"{base_name}-entities.txt"
    properties_file = f"{base_name}-properties.txt"
    literals_file = f"{base_name}-literals.txt"
    classes_file = f"{base_name}-classes.txt"

    data = None

    try:
        data = process_triples(
            file_path,
            f"{datasets_folder}/{dataset}/{entities_file}",
            f"{datasets_folder}/{dataset}/{properties_file}",
            f"{datasets_folder}/{dataset}/{literals_file}",
            f"{datasets_folder}/{dataset}/{classes_file}",
        )
    except Exception as e:
        log.error(f"{file_path} cannot be parsed: {str(e)}")

        # If an error occurs delete all the associated files
        for file in [entities_file, properties_file, literals_file, classes_file]:
            if os.path.exists(file):
                os.remove(file)

        return

    log.info(f"{file_path} processed")

    # create representation for the parsed dataset
    entry = {
        "file": file,
        "size": os.path.getsize(file_path),
        "classesFile": classes_file,
        "literalsFile": literals_file,
        "entitiesFile": entities_file,
        "propertiesFile": properties_file,
        "connections": data["connections"],
        "connectedVertices": data["connected_vertices"],
        "averageLiteralsPerVertex": data["average_literals_per_vertex"],
        "extractedWith": "lightrdf",
    }

    with open(metadata_file_path, "r+") as mf:
        data = json.load(mf, strict=False)

        data["unusedFiles"] = [e for e in data["unusedFiles"] if e["file"] != file]
        data["extracted"].append(entry)

        new_content = json.dumps(data, ensure_ascii=False, indent=4)

        mf.seek(0)
        mf.truncate(0)
        mf.write(new_content)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("folder", type=str, help="Folder in which datasets are stored")
    args = parser.parse_args()

    datasets_folder = args.folder

    logging.basicConfig(
        level=logging.INFO,
        filename="extract_stream.log",
        filemode="w",
        format="%(asctime)-15s %(levelname)-8s %(message)s",
    )

    log = logging.getLogger()

    # Files that have to be processed as streaming
    files_to_process = queue.Queue()

    # Fill the queue with files to process
    for dataset in os.listdir(datasets_folder):
        metadata_file_path = f"{datasets_folder}/{dataset}/metadata.json"

        with open(metadata_file_path, "r") as f:
            metadata = json.load(f, strict=False)

            keys = metadata.keys()
            if "unusedFiles" in keys and len(metadata["unusedFiles"]) > 0:
                for entry in metadata["unusedFiles"]:
                    file = entry["file"]

                    file_path = f"{datasets_folder}/{dataset}/{file}"
                    file_size = os.path.getsize(file_path)

                    file_extension = None

                    file_suffix = pathlib.Path(file_path).suffix.replace(".", "")
                    for ext in RDF_SUFFIXES:
                        if ext in file_suffix:
                            file_extension = ext

                    if file_extension is None:
                        log.warning(
                            f"{file_path} extension does not match allowed values"
                        )

                    if file_extension is not None and file_size > SIZE_LIMIT:
                        files_to_process.put((dataset, file))

    # Process the jobs from the queue sequentially
    while not files_to_process.empty():
        dataset, file = files_to_process.get()
        process_file(datasets_folder, dataset, file)

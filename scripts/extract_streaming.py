import os
import json
import queue
import argparse
import lightrdf
from statistics import mean
from collections import defaultdict

SIZE_LIMIT = 200 * 1024 * 1024  # 200 MB


def is_literal(node: str) -> bool:
    return node.startswith('"') and node.endswith('"')


def process_document(file_path: str) -> dict:
    doc = lightrdf.RDFDocument(file_path)

    entities = list()
    properties = list()
    literals = list()
    classes = list()

    number_of_connections = 0
    vertices_count_literals = defaultdict(int)

    for triple in doc.search_triples(None, None, None):
        sub = triple[0]
        prop = triple[1]
        obj = triple[2]

        entities.append(sub)

        if "type" in prop.lower() or "a" == prop.lower():
            classes.append(obj)
            continue

        properties.append(prop)

        is_obj_literal = is_literal(obj)

        if is_obj_literal:
            literals.append(obj)

        if not is_obj_literal:
            entities.append(obj)
            number_of_connections += 1

        if sub not in vertices_count_literals:
            vertices_count_literals[sub] = 0

        vertices_count_literals[sub] += 1

    connected_vertices = len(vertices_count_literals.keys())
    average_literals_per_vertex = mean(vertices_count_literals.values())

    return {
        "classes": classes,
        "literals": literals,
        "entities": entities,
        "properties": properties,
        "connections": number_of_connections,
        "connectedVertices": connected_vertices,
        "averageLiteralsPerVertex": average_literals_per_vertex,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("folder", type=str, help="Folder in which datasets are stored")
    args = parser.parse_args()

    dataset_folder = args.folder

    # Files that have to be processed as streaming
    files_to_process = queue.Queue()

    # Fill the queue with files to process
    for dataset in os.listdir(dataset_folder):
        metadata_file_path = f"{dataset_folder}/{dataset}/metadata.json"

        with open(metadata_file_path, "r") as f:
            metadata = json.load(f, strict=False)

            keys = metadata.keys()
            if "unusedFiles" in keys and len(metadata["unusedFiles"]) > 0:
                for file in metadata["unusedFiles"]:
                    file_path = f"{dataset_folder}/{dataset}/{file}"
                    file_size = os.path.getsize(file_path)

                    if file_size > SIZE_LIMIT:
                        files_to_process.put((dataset, file))

    # Process the jobs from the queue sequentially
    while not files_to_process.empty():
        dataset, file = files_to_process.get()

        file_path = f"{dataset_folder}/{dataset}/{file}"
        metadata_file = f"{dataset_folder}/{dataset}/metadata.json"

        data = None

        try:
            data = process_document(file_path)
        except Exception as e:
            print(f"ERROR while processing {file_path}")
            continue

        print(f"Processed file {file} for dataset {dataset}")

        # represent the data extracted from this file
        representation = dict()
        representation["file"] = file
        representation["fileSize"] = os.path.getsize(file_path)
        representation = representation | data
        representation["extractedWith"] = "lightrdf"

        with open(metadata_file, "r+") as f:
            # read JSON object from the file
            metadata = json.load(f, strict=False)

            # remove the file from the unused ones
            metadata["unusedFiles"].remove(file)

            # add the content extracted for the file
            metadata["extracted"].append(representation)

            # update the content of the metadata
            content = json.dumps(metadata, ensure_ascii=False, indent=4)

            # write to `metadata.json` with the new extracted data
            f.seek(0)  # return to the top of the file
            f.truncate(0)
            f.write(content)

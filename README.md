# AGE

Acordar Get and Extract is a python tool that retrieves the datasets from the ACORDAR collection and extracts from valid RDF files `literals, classes, entities , properties`

## Usage

Clone the ACORDAR repository
```sh
git clone https://github.com/nju-websoft/ACORDAR.git
```

Start the download of the files in the datasets (about 40GB of files to download sequentially)
```sh
python3 datasets_downloader.py ACORDAR/Data/datasets.json
```

Start the extraction of the data from the downloaded files (can take a long time)
```sh
time nice -n 19 python3 parallel_datasets_extractor.py --with-log datasets
```


### Directory structure created by `downloader.py`
```
datasets
├── 1
│   ├── curso-sf-dump.rdf
│   ├── curso-sf-dump.ttl
│   └── metadata.json
├── ...
└── 9995
    ├── metadata.json
    └── rows.rdf
```

### Example of `metadata.json` file
```json
{
    "dataset_id": "39",
    "title": "Finanzämter",
    "description": "Standorte und Kontaktdaten der Finanzämter in Schleswig-Holstein",
    "author": "",
    "tags": "finanzamt;zufish;",
    "downloaded_urls": [
        {
            "url": "https://opendata.zitsh.de/data/zufish/finanzamt_2020-01-07.rdf",
            "file_name": "finanzamt-2020-01-07.rdf"
        }
    ],
    "failed_download_urls": [],
    "classes": [
        "http://schema.org/GeoCoordinates",
        ...
    ],
    "literals": [
        "54.77532148075464",
        ...
    ],
    "entities": [
        "Nea8b52151b7e4a4c891e238f275b84ae",
        ...
    ],
    "properties": [
        "http://schema.org/longitude",
        ...
    ],
    "connections": 126,
    "connected_vertices": 76,
    "average_literals_per_vertex": 3.315,
    "used_files": [
        "finanzamt-2020-01-07.rdf"
    ],
    "unused_files": []
}
```

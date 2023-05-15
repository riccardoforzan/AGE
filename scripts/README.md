# Scripts

In this folder are the two scripts that download and extract data from the collection.

The instructions below assume that you are executing these scripts in the folder in which you want to download the file.

For example, if you simply clone this repo and execute the commands below, you will have a directory structure like this:
```
AGE
├── analysis
|   └── ...
├── docs
|   └── ...
├── experimental
|   └── ...
├── logs
|   └── ...
└── scripts
|   ├── ACORDAR
|       └── ...
|   ├── datasets
|       └── ...
|   ├── download.log
|   ├── downloader.py
|   ├── extract.log
|   ├── extract.py
|   ├── README.md
|   └── requirements.txt
├── utility
|   └── ...
├── .gitignore
├── README.md
```

### Preliminary step
Install all the required non-default packages using `pip install -r requirements.txt`

### Phase 1 - Get a hook to the ACORDAR repo
Clone the ACORDAR repository
```sh
git clone https://github.com/nju-websoft/ACORDAR.git
```

### Phase 2 - Download the collection
Start the download of the files in the datasets (about 40GB of files to download sequentially)
```sh
python3 datasets_downloader.py ACORDAR/Data/datasets.json
```

The execution of [downloader.py](./downloader.py) will create a folder `datasets` that stores all the downloaded files.
The new folder will be created in the directory in which the script is executed.

#### Directory structure created by `downloader.py`
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

### Phase 3 - Extract data from the downloaded files

Start the extraction of the data from the downloaded files (can take a long time)
```sh
time nice -n 19 python3 parallel_datasets_extractor.py datasets
```
The execution of this command creates a file `metadata.json` inside each dataset folder created by `downloader.py`.

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

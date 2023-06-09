# Scripts

In this folder are the two scripts that download and extract data from the collection.

The instructions below assume that you are executing these scripts in the folder in which you want to download the file.

For example, if you simply clone this repo and execute the commands below, you will have a directory structure like this:
```
AGE
├── analysis
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
|   ├── extract_stream.log
|   ├── extract_stream.py
|   ├── README.md
|   └── requirements.txt
├── utility
|   └── ...
├── .gitignore
├── README.md
```

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
time nice -n 19 python3 extract.py datasets
```
The execution of this command creates a file `metadata.json` inside each dataset folder created by `downloader.py`.

### Example of `metadata.json` file
```json
{
{
    "id": "32276",
    "title": "Nova Scotia Provincial Ambient Nitrogen Oxides (NOx, NO2, NO) Hourly Data Lake Major",
    "description": "Hourly ambient nitrogen oxides (NOx, NO2, NO) data in parts per billion from provincial ambient air quality monitoring stations across Nova Scotia up to the end of 2019.",
    "author": "Open Data Nova Scotia",
    "tags": "canadian ambient air quality standards;caaqs;air pollution;air quality;environmental monitoring;environmental reporting;nitrogen oxide;nitrogen dioxide;nox;no2;lake major",
    "downloadedURLs": [
        {
            "url": "https://data.novascotia.ca/resource/rqp4-39eg.rdf",
            "name": "rqp4-39eg.rdf"
        }
    ],
    "failedURLs": [],
    "extracted": [
        {
            "file": "rqp4-39eg.rdf",
            "size": 438242,
            "classesFile": "20230608-130714-rqp4-39eg-rdf-classes.txt",
            "literalsFile": "20230608-130714-rqp4-39eg-rdf-literals.txt",
            "entitiesFile": "20230608-130714-rqp4-39eg-rdf-entities.txt",
            "propertiesFile": "20230608-130714-rqp4-39eg-rdf-properties.txt",
            "connections": 2000,
            "connectedVertices": 1001,
            "averageLiteralsPerVertex": 5.957,
            "extractedWith": "RDFLib"
        }
    ],
    "unusedFiles": [
        {
            "file": "example.txt",
            "size": 61900
        }
    ]
}
}
```

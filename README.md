# datacatalog-object-storage-processor

A package for performing Data Catalog operations on object storage solutions.

[![CircleCI][3]][4] [![CoverageStatus][1]][2]

## 1. Environment setup

### 1.1. Get the code

````bash
git clone https://github.com/mesmacosta/datacatalog-object-storage-processor
cd datacatalog-object-storage-processor
````

### 1.2. Auth credentials

##### 1.2.1. Create a service account and grant it below roles

- Data Catalog Admin
- Storage Admin or Custom Role with storage.buckets.list acl

##### 1.2.2. Download a JSON key and save it as
- `./credentials/datacatalog-object-storage-processor-sa.json`

### 1.3. Virtualenv

Using *virtualenv* is optional, but strongly recommended unless you use [Docker](#24-docker).

##### 1.3.1. Install Python 3.6+

##### 1.3.2. Create and activate an isolated Python environment

```bash
pip install --upgrade virtualenv
python3 -m virtualenv --python python3 env
source ./env/bin/activate
```

##### 1.3.3. Install the dependencies

```bash
pip install --upgrade --editable .
```

##### 1.3.4. Set environment variables

```bash
export GOOGLE_APPLICATION_CREDENTIALS=./credentials/datacatalog-object-storage-processor-sa.json
```

### 1.4. Docker

Docker may be used as an alternative to run all the scripts. In this case, please disregard the [Virtualenv](#23-virtualenv) install instructions.

## 2. Create DataCatalog entries based on object storage files

### 2.1. python main.py 

- python

```bash
datacatalog-object-storage-processor \
  object-storage create-entries --type cloud-storage \
  --project-id my_project \
  --entry-group-name my_entry_group_name \
  --bucket-prefix my_bucket
```

- docker

```bash
docker build --rm --tag datacatalog-object-storage-processor .
docker run --rm --tty -v your_credentials_folder:/data datacatalog-object-storage-processor \
  --type cloud-storage \
  --project-id my_project \
  --entry-group-name my_entry_group_name \
  --bucket-prefix my_bucket
```

## 3 Delete up object storage entries on entry group
Delete entries for given entry group

```bash
datacatalog-object-storage-processor \
  object-storage delete-entries --type cloud-storage \
  --project-id my_project \
  --entry-group-name my_entry_group_name
```

## Disclaimers

This is not an officially supported Google product.

[1]: https://coveralls.io/repos/github/mesmacosta/datacatalog-object-storage-processor/badge.svg?branch=master&kill_cache=3
[2]: https://coveralls.io/github/mesmacosta/datacatalog-object-storage-processor?branch=master
[3]: https://circleci.com/gh/mesmacosta/datacatalog-object-storage-processor.svg?style=svg
[4]: https://circleci.com/gh/mesmacosta/datacatalog-object-storage-processor

# Data Manifest

DataManifest is a lightweight tool and python library for storing and versioning collections of bioinformatics data. The design philosphy is that everything should be usable as a file tree, while allowing for syncs to s3, fast cehckouts, and data safety through file-system level locks and md5sum verifications.

# Quick Start

### Configure S3 Access
DataManifest uses boto for communicating with S3. See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html for instructions on configuring boto. I also recommend installing aws-cli so that you can test the installation. 

### Install DataManifest

### Option  1: Conda
conda install -c nboley datamanifest


### Option 2: pip
Install all the dependencies in `etc/requirements.in`
run: `python setup.py install`


### Create a remote datastore

First, we'll create a new s3 bucket:
```
aws s3 mb s3://test-data-manifest-2-2024/
```

```
aws s3 ls s3://test-data-manifest-2-2024/
```
Shows that the bucket is empty.

### Populate a Directory with Data
```
mkdir test_data
cp ~/src/data_manifest/test/data/* test_data/
```

```
> ls -lt test_data/
total 708
-rw-rw-r-- 1 nboley developer 198736 Feb 29 10:56 small.chr6.bam
-rw-rw-r-- 1 nboley developer  97152 Feb 29 10:56 small.chr6.bam.bai
-rw-rw-r-- 1 nboley developer 283499 Feb 29 10:56 GRCh38.p12.genome.chr6_99110000_99130000.fa.gz
-rw-rw-r-- 1 nboley developer     37 Feb 29 10:56 GRCh38.p12.genome.chr6_99110000_99130000.fa.gz.fai
-rw-rw-r-- 1 nboley developer  41864 Feb 29 10:56 GRCh38.p12.genome.chr6_99110000_99130000.fa.gz.gzi
-rw-rw-r-- 1 nboley developer  36242 Feb 29 10:56 scATAC_breast_v1_chr6_99118615_99121634.hg38.bam
-rw-rw-r-- 1 nboley developer  50032 Feb 29 10:56 scATAC_breast_v1_chr6_99118615_99121634.hg38.bam.bai
```

### Create a New Data Manifest


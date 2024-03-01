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

Make a directory to store the data and populate it. In my case I'll use the test data from another project.
```
mkdir test_data
cp ~/src/fragments_h5/test/data/* test_data/
```

`ls -lt` shows the files and sizes. 

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

### Using the command line interface

Running `dm --help` shows all of the sub-commands and options. 
Running `dm sub_cmd --help` shows the options for that particular subcommand. 
Global options like `--vebrose` and `--quiet` need to be passed after `dm` but before the sub-command. e.g. `dm --verbose create` not `dm create --verbose`.


### Create a new data manifest

Create a new data manifest. Note that we need to pass LOCAL_DATA_MIRROR_PATH and REMOTE_DATA_MIRROR_URI as environment variables. Usually these would be set in your `.bashrc` or similar.
```
> LOCAL_DATA_MIRROR_PATH=/tmp/test_dm/ REMOTE_DATA_MIRROR_URI=s3://test-data-manifest-2-2024/test1 dm create ./test.data_manifest.tsv ./test_checkout/ ./test_data/*
Add files: 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 4/4 [00:01<00:00,  3.95it/s]
```

Now that the files are added to the manifest we can look at the data manifest and the checkout directory:
```
> cat test.data_manifest.tsv
key     md5sum  size    notes
data/small.chr6.bam.bai 69ef0af03399b9cfe7037aaaa5cdff7b        97152
data/small.chr6.bam     100d7d094d19c7eaa2b93a4bb67ecda7        198736
genome/GRCh38.p12.genome.chr6_99110000_99130000.fa.gz   f02b28cef526d5ee3d657f010bfbc2bb        283499
README  ca1ea02c10b7c37f425b9b7dd86d5e11        9

> find test_checkout/
test_checkout/
test_checkout/genome
test_checkout/genome/GRCh38.p12.genome.chr6_99110000_99130000.fa.gz
test_checkout/README
test_checkout/data
test_checkout/data/small.chr6.bam.bai
test_checkout/data/small.chr6.bam
```

Note that the files under `test_checkout` are symbolic links to LOCAL_DATA_MIRROR_PATH to faciliate fast checkouts. 
```
> stat test_checkout/README 
  File: test_checkout/README -> /tmp/test_dm/ca1ea02c10b7c37f425b9b7dd86d5e11-README
  Size: 52              Blocks: 0          IO Block: 4096   symbolic link
Device: 900h/2304d      Inode: 488968007   Links: 1
Access: (0777/lrwxrwxrwx)  Uid: ( 1001/  nboley)   Gid: ( 5018/developer)
Access: 2024-02-29 12:58:04.886945490 -0800
Modify: 2024-02-29 12:56:51.020217023 -0800
Change: 2024-02-29 12:56:51.020217023 -0800
 Birth: -
```

The files have also been mirrored in s3:
```
> aws s3 ls --recursive test-data-manifest-2-2024
2024-02-29 12:59:53          9 test1/ca1ea02c10b7c37f425b9b7dd86d5e11-README
2024-02-29 12:59:52     198736 test1/data/100d7d094d19c7eaa2b93a4bb67ecda7-small.chr6.bam
2024-02-29 12:59:51      97152 test1/data/69ef0af03399b9cfe7037aaaa5cdff7b-small.chr6.bam.bai
2024-02-29 12:59:52     283499 test1/genome/f02b28cef526d5ee3d657f010bfbc2bb-GRCh38.p12.genome.chr6_99110000_99130000.fa.gz
```

Note that the filenames in both mirrors have been pre-pended with their md5 checksums to allow for versioning.

### Checkout an existing data manifest

```
LOCAL_DATA_MIRROR_PATH=/tmp/test_dm/ REMOTE_DATA_MIRROR_URI=s3://test-data-manifest-2-2024/test1 dm checkout test.data_manifest.tsv test_checkout_2
100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 4/4 [00:00<00:00, 844.01it/s]
```

Verifying that the links were all created:
```
find test_checkout_2/
test_checkout_2/
test_checkout_2/genome
test_checkout_2/genome/GRCh38.p12.genome.chr6_99110000_99130000.fa.gz
test_checkout_2/README
test_checkout_2/data
test_checkout_2/data/small.chr6.bam.bai
test_checkout_2/data/small.chr6.bam
```

You can use the `--fast` option to skip verifying the md5sums on checkout (it will still verify that the file sizes match so this is pretty safe).


# Gotchas and Caveats

### A couple things to note about create:

#### Use `--verbose` with the `--dry-run` option to check what keys and files will be processed. e.g.:
```
> LOCAL_DATA_MIRROR_PATH=/tmp/test_dm/ REMOTE_DATA_URI=s3://test-data-manifest-2-2024/test1 dm --verbose create ./test.data_manifest.tsv ./test_checkout/ ./test_data/ --dry-run
[__main__ : 2024-02-29 12:48:22,915 dm - _find_all_files_and_directories() ] Adding 'test_data' to ./tmp.LMpMSiDiK2FhxsKc.test.data_manifest.tsv
Add files:   0%|                                                                                                                                                                                                                                                                                                                                  | 0/4 [00:00<?, ?it/s][__main__ : 2024-02-29 12:48:22,916 dm - _add_subdirectory() ] Adding 'test_data/README' to ./tmp.LMpMSiDiK2FhxsKc.test.data_manifest.tsv for /scratch/nboley/dm_tests/test_data/README
[__main__ : 2024-02-29 12:48:22,916 dm - _add_subdirectory() ] Adding 'test_data/genome/GRCh38.p12.genome.chr6_99110000_99130000.fa.gz' to ./tmp.LMpMSiDiK2FhxsKc.test.data_manifest.tsv for /scratch/nboley/dm_tests/test_data/genome/GRCh38.p12.genome.chr6_99110000_99130000.fa.gz
[__main__ : 2024-02-29 12:48:22,916 dm - _add_subdirectory() ] Adding 'test_data/data/small.chr6.bam.bai' to ./tmp.LMpMSiDiK2FhxsKc.test.data_manifest.tsv for /scratch/nboley/dm_tests/test_data/data/small.chr6.bam.bai
[__main__ : 2024-02-29 12:48:22,916 dm - _add_subdirectory() ] Adding 'test_data/data/small.chr6.bam' to ./tmp.LMpMSiDiK2FhxsKc.test.data_manifest.tsv for /scratch/nboley/dm_tests/test_data/data/small.chr6.bam
Add files: 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 4/4 [00:00<00:00, 31126.56it/s]
```


#### The key is inferred from the passed directory structure.

`dm create ./test.data_manifest.tsv ./test_checkout/ ./test_data/*` yields:
```
key     md5sum  size    notes
data/small.chr6.bam.bai 69ef0af03399b9cfe7037aaaa5cdff7b        97152
data/small.chr6.bam     100d7d094d19c7eaa2b93a4bb67ecda7        198736
genome/GRCh38.p12.genome.chr6_99110000_99130000.fa.gz   f02b28cef526d5ee3d657f010bfbc2bb        283499
README  ca1ea02c10b7c37f425b9b7dd86d5e11        9
```

whereas `dm create ./test.data_manifest.tsv ./test_checkout/ ./test_data/` yields:
```
key     md5sum  size    notes
test_data/README        ca1ea02c10b7c37f425b9b7dd86d5e11        9
test_data/genome/GRCh38.p12.genome.chr6_99110000_99130000.fa.gz f02b28cef526d5ee3d657f010bfbc2bb        283499
test_data/data/small.chr6.bam.bai       69ef0af03399b9cfe7037aaaa5cdff7b        97152
test_data/data/small.chr6.bam   100d7d094d19c7eaa2b93a4bb67ecda7        198736
```



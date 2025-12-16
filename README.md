# Data Manifest

DataManifest is a lightweight tool and python library for storing and versioning collections of bioinformatics data. The design philosophy is that everything should be usable as a file tree, while allowing for syncs to S3, fast checkouts, and data safety through file-system level locks and md5sum verifications.

## Versioning

DataManifest uses **S3 native versioning** to track file versions. This means:
- The S3 bucket **must have versioning enabled** before creating a manifest
- Remote files are stored with their original names (no MD5 prefix)
- Each file upload returns a unique S3 version ID that is stored in the manifest
- Previous versions remain accessible via their version IDs
- Local cache files use MD5 prefixes for deduplication across versions

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

First, we'll create a new S3 bucket with versioning enabled:
```
aws s3 mb s3://TEST-DATA-MANIFEST/
aws s3api put-bucket-versioning --bucket TEST-DATA-MANIFEST --versioning-configuration Status=Enabled
```

Verify versioning is enabled:
```
aws s3api get-bucket-versioning --bucket TEST-DATA-MANIFEST
{
    "Status": "Enabled"
}
```

### Run tests to verify the installation
```pytest --verbose .```

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
Global options like `--verbose` and `--quiet` need to be passed after `dm` but before the sub-command. e.g. `dm --verbose create` not `dm create --verbose`.


### Create a new data manifest

Create a new data manifest:
```
> dm create test.data_manifest.tsv --checkout-prefix ./test_checkout/ --remote-datastore-uri s3://TEST-DATA-MANIFEST/test1  ./test_data/*
Add files: 100%|████████████████████████████████████████████████████████████████| 4/4 [00:01<00:00,  3.95it/s]
```

`dm create` creates two metadata files:
- A data manifest tsv that stores remote config info and the file information (`test.data_manifest.tsv`)
- A local config file that stores the checkout directory and local cache path (`test.data_manifest.tsv.local_config`)

We can look at the data manifest, the local config file, and the checkout directory:
```
> cat test.data_manifest.tsv
#MANIFEST_VERSION=2
#REMOTE_DATA_MIRROR_URI=s3://TEST-DATA-MANIFEST/test1
#LOCAL_CACHE_PATH_SUFFIX=./DATA_MANIFEST_CACHE_jvHksagknpbD8Cmq/
key	s3_version_id	md5sum	size	notes
data/small.chr6.bam.bai	abc123def456...	69ef0af03399b9cfe7037aaaa5cdff7b	97152
data/small.chr6.bam	ghi789jkl012...	100d7d094d19c7eaa2b93a4bb67ecda7	198736
genome/GRCh38.p12.genome.chr6_99110000_99130000.fa.gz	mno345pqr678...	f02b28cef526d5ee3d657f010bfbc2bb	283499
README	stu901vwx234...	ca1ea02c10b7c37f425b9b7dd86d5e11	9

> cat test.data_manifest.tsv.local_config 
MANIFEST_VERSION=2
CHECKOUT_PREFIX=/scratch/nboley/dm_tests/test_checkout
LOCAL_CACHE_PREFIX=/tmp/DATA_MANIFEST_CACHE_jvHksagknpbD8Cmq

> find test_checkout/
test_checkout/
test_checkout/genome
test_checkout/genome/GRCh38.p12.genome.chr6_99110000_99130000.fa.gz
test_checkout/README
test_checkout/data
test_checkout/data/small.chr6.bam.bai
test_checkout/data/small.chr6.bam
```

Note that the files under `test_checkout` are symbolic links to the local cache to facilitate fast checkouts. 
```
> stat test_checkout/README 
  File: test_checkout/README -> /tmp/DATA_MANIFEST_CACHE_jvHksagknpbD8Cmq/ca1ea02c10b7c37f425b9b7dd86d5e11-README
  Size: 52              Blocks: 0          IO Block: 4096   symbolic link
```

Files containing the data can be found under the local_cache_prefix. Note that **local cache files have MD5 prefixes** for deduplication:
```
> find /tmp/DATA_MANIFEST_CACHE_jvHksagknpbD8Cmq
/tmp/DATA_MANIFEST_CACHE_jvHksagknpbD8Cmq
/tmp/DATA_MANIFEST_CACHE_jvHksagknpbD8Cmq/genome
/tmp/DATA_MANIFEST_CACHE_jvHksagknpbD8Cmq/genome/f02b28cef526d5ee3d657f010bfbc2bb-GRCh38.p12.genome.chr6_99110000_99130000.fa.gz
/tmp/DATA_MANIFEST_CACHE_jvHksagknpbD8Cmq/data
/tmp/DATA_MANIFEST_CACHE_jvHksagknpbD8Cmq/data/69ef0af03399b9cfe7037aaaa5cdff7b-small.chr6.bam.bai
/tmp/DATA_MANIFEST_CACHE_jvHksagknpbD8Cmq/data/100d7d094d19c7eaa2b93a4bb67ecda7-small.chr6.bam
/tmp/DATA_MANIFEST_CACHE_jvHksagknpbD8Cmq/ca1ea02c10b7c37f425b9b7dd86d5e11-README
```

The files have also been mirrored in S3. Note that **remote files use plain paths** (S3 versioning handles versions):
```
> aws s3 ls --recursive TEST-DATA-MANIFEST
2024-02-29 12:59:53          9 test1/README
2024-02-29 12:59:52     198736 test1/data/small.chr6.bam
2024-02-29 12:59:51      97152 test1/data/small.chr6.bam.bai
2024-02-29 12:59:52     283499 test1/genome/GRCh38.p12.genome.chr6_99110000_99130000.fa.gz
```

To see all versions of a file, use `aws s3api list-object-versions`:
```
> aws s3api list-object-versions --bucket TEST-DATA-MANIFEST --prefix test1/README
```


### Checkout and sync an existing data manifest

The data manifest stores all of the information needed to re-create the data in a new environment. 

To checkout an existing data manifest in a new environment use the `dm checkout` command. In our case, we will delete the associated local_config file to simulate a new environment.

```
rm test.data_manifest.tsv.local_config
> dm checkout test.data_manifest.tsv --checkout-prefix test_checkout_2
[__main__ : 2024-03-01 07:54:49,258 dm - parse_args() ] Changing the relative checkout prefix path 'test_checkout_2' to '/scratch/nboley/dm_tests/test_checkout_2'
```

`checkout` is a lazy command meaning that it creates the local config file and checkout directory but doesn't sync any files. We can sync by either using the sync command or passing the `--sync` option to `dm checkout`. 

To sync the newly checkout directory run:
```
> dm sync test.data_manifest.tsv
100%|███████████████████████████████████████████████████████████████████████████| 4/4 [00:00<00:00, 790.71it/s]
```

Verifying that the links were all created and match test_checkout/:
```
> diff -r test_checkout/ test_checkout_2/
> find test_checkout_2/
test_checkout_2/
test_checkout_2/genome
test_checkout_2/genome/GRCh38.p12.genome.chr6_99110000_99130000.fa.gz
test_checkout_2/README
test_checkout_2/data
test_checkout_2/data/small.chr6.bam.bai
test_checkout_2/data/small.chr6.bam
```

Note that the files in `test_checkout/` and `test_checkout_2/` point to the same files in the cache. e.g.:

```
> stat test_checkout/README | head -n 1
  File: test_checkout/README -> /tmp/DATA_MANIFEST_CACHE_jvHksagknpbD8Cmq/ca1ea02c10b7c37f425b9b7dd86d5e11-README
> stat test_checkout_2/README | head -n 1
  File: test_checkout_2/README -> /tmp/DATA_MANIFEST_CACHE_jvHksagknpbD8Cmq/ca1ea02c10b7c37f425b9b7dd86d5e11-README
```

This means that running a checkout on a system with a shared cache is a very inexpensive operation -- it just requires verifying the md5sums and file sizes.  You can use the `--fast` option to make this even faster by skipping verifying the md5sums on checkout (it will still verify that the file sizes match so this is still pretty safe).

One common use case for a shared cache is using docker containers. A shared physical cache can be mounted in multiple docker containers, and then files can be checked out very quickly without needing to actually download or copy data.

### Adding, Updating, and Deleting files

`dm` has commands for adding, updating, and deleting files. 

First we'll create a test file:
```
> echo 'AAAAAAAAA' > TENAs.txt
```

Then we'll add to the data manifest with key test_key:
```
> dm add test.data_manifest.tsv test_key TENAs.txt
> cat test.data_manifest.tsv | tail -n 1
test_key	xyz789abc123...	f252b28c22d0bb68caf870df063b6064	10
```

We can also update the file:
```
> echo 'BBBBBBBBB' > TENAs.txt
> dm update test.data_manifest.tsv test_key TENAs.txt
> cat test.data_manifest.tsv | tail -n 1
test_key	def456ghi789...	961310d0926542e45d7190a22d68b48c	10
```

Note the change in both the s3_version_id and md5sum. Both versions still exist in S3 (accessible via their version IDs) and in the local cache:
```
> find /tmp/DATA_MANIFEST_CACHE_jvHksagknpbD8Cmq | grep test_key
/tmp/DATA_MANIFEST_CACHE_jvHksagknpbD8Cmq/961310d0926542e45d7190a22d68b48c-test_key
/tmp/DATA_MANIFEST_CACHE_jvHksagknpbD8Cmq/f252b28c22d0bb68caf870df063b6064-test_key

> aws s3api list-object-versions --bucket TEST-DATA-MANIFEST --prefix test1/test_key
{
    "Versions": [
        {
            "VersionId": "def456ghi789...",
            "Key": "test1/test_key",
            ...
        },
        {
            "VersionId": "xyz789abc123...",
            "Key": "test1/test_key",
            ...
        }
    ]
}
```

Finally, we can delete files from the manifest:
```
> dm delete test.data_manifest.tsv test_key
> cat test.data_manifest.tsv
#MANIFEST_VERSION=2
#REMOTE_DATA_MIRROR_URI=s3://TEST-DATA-MANIFEST/test1
#LOCAL_CACHE_PATH_SUFFIX=./DATA_MANIFEST_CACHE_jvHksagknpbD8Cmq/
key	s3_version_id	md5sum	size	notes
data/small.chr6.bam.bai	abc123def456...	69ef0af03399b9cfe7037aaaa5cdff7b	97152
data/small.chr6.bam	ghi789jkl012...	100d7d094d19c7eaa2b93a4bb67ecda7	198736
genome/GRCh38.p12.genome.chr6_99110000_99130000.fa.gz	mno345pqr678...	f02b28cef526d5ee3d657f010bfbc2bb	283499
README	stu901vwx234...	ca1ea02c10b7c37f425b9b7dd86d5e11	9
```

Note that `test_key` is no longer in the manifest. By default, `dm delete` only removes the file from the manifest. The file still exists in S3 (both versions) and in the local cache. This allows you to restore the file later if needed.

To permanently delete the specific version from S3, use `--delete-from-datastore`:
```
> dm delete test.data_manifest.tsv test_key --delete-from-datastore
WARNING: you have chosen to delete 'test_key' from the datastore.
This action CANNOT BE UNDONE!
Type 'I am sure' to continue: I am sure
```

This permanently deletes the specific version from S3 (other versions of the same key remain accessible).

# Gotchas and Caveats

### S3 Bucket Versioning Required

DataManifest requires S3 bucket versioning to be enabled. If you try to create a manifest pointing to a bucket without versioning, you'll get an error:
```
RuntimeError: S3 bucket 'my-bucket' does not have versioning enabled. 
Please enable versioning on the bucket before creating a data manifest.
```

Enable versioning with:
```
aws s3api put-bucket-versioning --bucket my-bucket --versioning-configuration Status=Enabled
```

### A couple things to note about create:

#### Use `--verbose` with the `--dry-run` option to check what keys and files will be processed. e.g.:
```
> dm --verbose create ./test.data_manifest.tsv --checkout-prefix ./test_checkout/ --remote-datastore-uri s3://TEST-DATA-MANIFEST/test1 ./test_data/ --dry-run
```

#### The key is inferred from the passed directory structure.

`dm create ./test.data_manifest.tsv --checkout-prefix ./test_checkout/ --remote-datastore-uri s3://bucket/path ./test_data/*` yields:
```
key	s3_version_id	md5sum	size	notes
data/small.chr6.bam.bai	...	69ef0af03399b9cfe7037aaaa5cdff7b	97152
data/small.chr6.bam	...	100d7d094d19c7eaa2b93a4bb67ecda7	198736
genome/GRCh38.p12.genome.chr6_99110000_99130000.fa.gz	...	f02b28cef526d5ee3d657f010bfbc2bb	283499
README	...	ca1ea02c10b7c37f425b9b7dd86d5e11	9
```

whereas `dm create ./test.data_manifest.tsv --checkout-prefix ./test_checkout/ --remote-datastore-uri s3://bucket/path ./test_data/` yields:
```
key	s3_version_id	md5sum	size	notes
test_data/README	...	ca1ea02c10b7c37f425b9b7dd86d5e11	9
test_data/genome/GRCh38.p12.genome.chr6_99110000_99130000.fa.gz	...	f02b28cef526d5ee3d657f010bfbc2bb	283499
test_data/data/small.chr6.bam.bai	...	69ef0af03399b9cfe7037aaaa5cdff7b	97152
test_data/data/small.chr6.bam	...	100d7d094d19c7eaa2b93a4bb67ecda7	198736
```

### Manifest Version

DataManifest includes a `MANIFEST_VERSION` in both the manifest file and local config file. This ensures compatibility and will produce clear error messages if you try to use an incompatible manifest format.


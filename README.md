# Data Manifest

DataManifest is a lightweight tool and python library for storing and versioning collections of bioinformatics data. The design philosphy is that everything should be usable as a file tree, while allowing for syncs to s3, fast cehckouts, and data safety through file-system level locks and md5sum verifications.

# Quick Start

### Configure S3 Access
DataManifest uses boto for communicating with S3. See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html for instructions on vonfiguring boto.

After S3 access is configured, you'll need to specify an S3 URI for the remote datastore. 

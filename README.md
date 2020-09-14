# s3_quest

## Getting Started (Python3)

python libaries:

```
boto3 - pip3 install boto3
bs4 - pip3 install bs4
```

Update below values(runner.py):

```
aws_access_key_id='your_key_id',
aws_secret_access_key='your_access_key')
bucket = s3.Bucket('your_bucket')
```

To run sync with python:

```
python3 runner.py
```

To run sync with sh:

```
sh run_sync.sh
```

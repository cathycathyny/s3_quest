from sync_data import sync
import boto3

session = boto3.Session(
    aws_access_key_id='your_key_id',
    aws_secret_access_key='your_access_key')

s3 = session.resource('s3')


def run():
    client = boto3.client('s3')
    bucket = s3.Bucket('your_bucket')
    sync(client, bucket)


if __name__ == "__main__":
    run()

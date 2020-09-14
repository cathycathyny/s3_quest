import bs4
import requests
from bs4 import BeautifulSoup


"""
This function is used to collect all the filename and details about the file (last modified data&length)

return: {filename: {last_modified_date:, length:}}
"""


def collect_files(root, path):
    url = root+path
    r = requests.get(url, allow_redirects=True)
    soup = BeautifulSoup(r.content, features="lxml")
    files = []
    file_meta = []
    dt = soup.pre
    for x in dt.contents[1:]:
        if getattr(x, 'name') != 'br':
            if type(x) == bs4.element.NavigableString:
                t = x.string.split()
                file_meta.append(
                    {'last_modified_date': ' '.join(t[:3]), 'length': t[-1]})
            if type(x) == bs4.element.Tag:
                files.append(x.get('href').replace(path, ''))
    all_files = dict(zip(files, file_meta))
    return all_files


"""
function to go to the file link and save the content to s3 bucket
"""


def upload(client, url, filename, details):
    r = requests.get(url+filename, allow_redirects=True)
    date_str = details['last_modified_date']
    content_length = details['length']
    client.put_object(
        Bucket='rearc-quest',
        Body=r.content,
        Key=filename,
        Metadata={
            'Content-Type': r.headers['content-type'],
            'Content-Length': content_length,
            'Last-Modified': date_str,
        }
    )
    print(f"{filename} is uploaded.")


"""
Main function to sync the data source to s3 bucket
"""


def sync(client, bucket):
    root = 'https://download.bls.gov'
    path = '/pub/time.series/ap/'
    all_files = collect_files(root, path)
    # when no files in the bucket
    if len(list(bucket.objects.all())) == 0:
        for filename in all_files:
            upload(client, root+path, filename, all_files[filename])
    else:
        existed_files = []
        for s3_file in bucket.objects.all():
            existed_files.append(s3_file.key)
            if s3_file.key in all_files:
                response = client.get_object(
                    Bucket=s3_file.bucket_name, Key=s3_file.key)
                # check last modified date & check length, if they don't have the same value, upload the new one
                if all_files[s3_file.key]['last_modified_date'] != response['Metadata']['last-modified'] or all_files[s3_file.key]['length'] != response['Metadata']['content-length']:
                    upload(client, root+path, s3_file.key,
                           all_files[s3_file.key])
                else:
                    print(f"{s3_file.key} no change.")
            else:
                # if file is removed from the data source, remove the file from bucket
                client.delete_object(
                    Bucket=s3_file.bucket_name, Key=s3_file.key)
                print(f"{s3_file.key} is removed.")
        # upload new file that not in the bucket yet
        for filename in list(set(all_files.keys()).difference(set(existed_files))):
            upload(client, root+path, filename, all_files[filename])

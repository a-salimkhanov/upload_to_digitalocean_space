#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import boto3
import mimetypes
from botocore.client import Config
from tqdm import tqdm

# DigitalOcean credentials
REGION = 'fra1'
ENDPOINT = 'https://fra1.digitaloceanspaces.com'
BUCKET_NAME = 'my_bucket'
ACCESS_ID = 'my_id'
SECRET_KEY = 'my_key'


LOCAL_DIR = '/2022/10/'
REMOTE_DIR = 'images/2022/10/' # If no such folder, it will be created automatically
LIMIT = False # Integer, Limit for uploaded files count. Set "False", if no limit required
IGNORE_EXTENSIONS = ['heic']


# Initialize a session using DigitalOcean Spaces.
session = boto3.session.Session()
client = session.client('s3',
    region_name=REGION,
    endpoint_url=ENDPOINT,
    aws_access_key_id=ACCESS_ID,
    aws_secret_access_key=SECRET_KEY)

count = 0
files_to_upload = []
duplicate_files = []
remote_file_list = []

print('Getting remote file list...')
paginator = client.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=REMOTE_DIR)

for page in pages:
    if 'Contents' in page:
        remote_file_list += [obj['Key'].split('/')[-1] for obj in page["Contents"]]

print(f'Remote files: {len(remote_file_list)}')

for filename in os.listdir(LOCAL_DIR):
    # check if current path is a file
    if (not filename.split('.')[-1] in IGNORE_EXTENSIONS
        and os.path.isfile(os.path.join(LOCAL_DIR, filename))):
        if filename in remote_file_list:
            duplicate_files.append(filename)
        else:
            if LIMIT and count == LIMIT:
                break
            count += 1
            files_to_upload.append(filename)

print(f'Found duplicate files: {len(duplicate_files)}')
print(f'File to upload: {len(files_to_upload)}')

for filename in tqdm(files_to_upload):
    try:
        mimetype = ['image/webp'] if filename.split('.')[-1] == 'webp' else mimetypes.guess_type(filename)
        client.upload_file(
            f'{LOCAL_DIR}{filename}',
            BUCKET_NAME,
            f'{REMOTE_DIR}{filename}',
            ExtraArgs={'ACL':'public-read', 'ContentType': mimetype[0]}
        )
    except Exception as e:
        print(f"Couldn't upload {filename} {e}")

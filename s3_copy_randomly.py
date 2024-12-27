import boto3
import random
from botocore.exceptions import NoCredentialsError

def list_objects(s3_client, bucket_name, prefix):
    """List all objects in a bucket with the given prefix."""
    print(f"Listing objects in bucket '{bucket_name}' with prefix '{prefix}'...")
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    objects = []
    for page in pages:
        objects.extend(page.get('Contents', []))
    print(f"Total objects found: {len(objects)}")
    return objects

def copy_file(s3_client, source_bucket, source_key, target_bucket, target_key):
    """Copy a file from one bucket to another."""
    print(f"Attempting to copy file '{source_key}' from '{source_bucket}' to '{target_key}' in '{target_bucket}'...")
    try:
        s3_client.head_object(Bucket=target_bucket, Key=target_key)
        print(f"File already exists at destination: {target_key}. Skipping.")
    except:
        copy_source = {'Bucket': source_bucket, 'Key': source_key}
        s3_client.copy(copy_source, target_bucket, target_key)
        print(f"File copied successfully: {source_key} to {target_key}")

def copy_related_thumbs(s3_client, source_bucket, target_bucket, directory_prefix):
    """Copy all .jpg files related to a directory prefix to the destination bucket."""
    print(f"Looking for thumbnail files in '{source_bucket}' with prefix '{directory_prefix}'...")
    objects = list_objects(s3_client, source_bucket, directory_prefix)
    thumbs_copied = 0
    for obj in objects:
        if obj['Key'].endswith('.jpg'):
            source_key = obj['Key']
            target_key = obj['Key']
            copy_file(s3_client, source_bucket, source_key, target_bucket, target_key)
            thumbs_copied += 1
    print(f"Total thumbnails copied: {thumbs_copied}")

def copy_random_files(
    source_bucket, 
    target_bucket, 
    prefix, 
    s3_client, 
    thumb_source_bucket, 
    thumb_target_bucket, 
    num_files=5
):
    """Randomly copy `num_files` files from third-level directories to another bucket."""
    print(f"Starting random file copy process with prefix '{prefix}'...")
    level_2_prefixes = set()

    # List objects in the source bucket
    objects = list_objects(s3_client, source_bucket, prefix)

    # Find level 2 prefixes
    for obj in objects:
        key_parts = obj['Key'].split('/')
        if len(key_parts) >= 3:
            level_2_prefixes.add(f"{key_parts[0]}/{key_parts[1]}")

    for level_2_prefix in level_2_prefixes:
        print(f"Processing level 2 prefix: {level_2_prefix}...")
        level_3_dirs = set()
        for obj in objects:
            if obj['Key'].startswith(level_2_prefix):
                key_parts = obj['Key'].split('/')
                if len(key_parts) >= 4:
                    level_3_dirs.add(f"{key_parts[0]}/{key_parts[1]}/{key_parts[2]}")

        level_3_dirs = list(level_3_dirs)
        if len(level_3_dirs) < num_files:
            print(f"Warning: Only {len(level_3_dirs)} directories available in {level_2_prefix}, less than the required ({num_files}).")

        selected_dirs = random.sample(level_3_dirs, min(len(level_3_dirs), num_files))

        for dir_prefix in selected_dirs:
            print(f"Processing directory: {dir_prefix}...")
            for obj in objects:
                if obj['Key'].startswith(dir_prefix) and obj['Key'].endswith('.mp4'):
                    source_key = obj['Key']
                    target_key = obj['Key']

                    # Copy the video file
                    copy_file(s3_client, source_bucket, source_key, target_bucket, target_key)

                    # Adjust directory prefix to locate thumbnails
                    thumb_prefix = '/'.join(source_key.split('/')[:-2]) + '/'
                    copy_related_thumbs(s3_client, thumb_source_bucket, thumb_target_bucket, thumb_prefix)

if __name__ == "__main__":
    source_bucket = "random-copy-23-12-2024"
    target_bucket = "random-copy-poc-23-12-2024-output"
    thumb_source_bucket = "caracol-image-ingest-prod"
    thumb_target_bucket = "random-copy-poc-23-12-2024-output-image"
    prefix = ""  # Initial prefix, if any

    try:
        s3_client = boto3.client('s3')
        copy_random_files(
            source_bucket, 
            target_bucket, 
            prefix, 
            s3_client, 
            thumb_source_bucket, 
            thumb_target_bucket, 
            num_files=5
        )
    except NoCredentialsError:
        print("AWS credentials not found. Make sure they are configured correctly.")
    except Exception as e:
        print(f"Error executing the script: {e}")

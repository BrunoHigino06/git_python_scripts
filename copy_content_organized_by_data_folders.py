import boto3
import time
from datetime import datetime
from botocore.exceptions import ClientError

# Configuration
SOURCE_BUCKET = 'thumbnail-image-poc'   # Replace with the source bucket name
DEST_BUCKET = 'thumbnail-on-demand'  # Replace with the destination bucket name

# Variables for sleep settings
BATCH_SIZE = 1  # Number of files to copy before sleeping
SLEEP_TIME_MINUTES = 1  # Time to sleep in minutes

# Initialize S3 client (credentials are automatically loaded from CLI)
s3_client = boto3.client('s3')

# Function to get the list of folders (date-based) and return them sorted by date
def get_sorted_folders(bucket):
    try:
        # List all the top-level folders in the source bucket
        result = s3_client.list_objects_v2(Bucket=bucket, Delimiter='/')

        if 'CommonPrefixes' not in result:
            print(f"No folders found in bucket {bucket}")
            return []

        # Extract folder names (should be in the format dd-mm-yyyy)
        folder_names = [prefix['Prefix'].strip('/') for prefix in result['CommonPrefixes']]

        # Sort the folder names by date (assuming folder names are in dd-mm-yyyy format)
        folder_names.sort(key=lambda date: datetime.strptime(date, '%d-%m-%Y'))
        print(f"Sorted folders by date: {folder_names}")
        return folder_names

    except ClientError as e:
        print(f"Error fetching folders from bucket {bucket}: {e}")
        return []

# Function to copy files with a limit on the number of files copied per batch
def copy_files_from_folder(source_bucket, folder, dest_bucket):
    print(f"Copying files from folder {folder} in {source_bucket} to {dest_bucket}...")
    
    try:
        # Use paginator to list all objects within the specific folder
        paginator = s3_client.get_paginator('list_objects_v2')
        total_files_copied = 0
        
        for page in paginator.paginate(Bucket=source_bucket, Prefix=f"{folder}/"):
            if 'Contents' not in page:
                print(f"No files found in folder {folder}")
                continue

            # Copy each file in the folder to the destination bucket root
            for item in page['Contents']:
                file_key = item['Key']
                print(f"Processing file: {file_key}")

                # Define the destination key (copy to the root, so only keep the filename)
                destination_key = file_key.split('/')[-1]

                try:
                    copy_source = {'Bucket': source_bucket, 'Key': file_key}
                    s3_client.copy(copy_source, dest_bucket, destination_key)
                    print(f"Copied {file_key} to {dest_bucket}/{destination_key}")
                    total_files_copied += 1

                    # If we reach the batch size, sleep for the specified time
                    if total_files_copied % BATCH_SIZE == 0:
                        print(f"Copied {total_files_copied} files, sleeping for {SLEEP_TIME_MINUTES} minutes...")
                        time.sleep(SLEEP_TIME_MINUTES * 60)  # Sleep for specified minutes

                except ClientError as e:
                    print(f"Error copying {file_key}: {e}")

    except ClientError as e:
        print(f"Error copying files from folder {folder}: {e}")

# Main script execution
if __name__ == '__main__':
    print("Starting the script to copy files from folders by date...")

    # Get the sorted folder list from the source bucket
    sorted_folders = get_sorted_folders(SOURCE_BUCKET)

    # Copy files from each folder to the destination bucket
    for folder in sorted_folders:
        copy_files_from_folder(SOURCE_BUCKET, folder, DEST_BUCKET)

    print("Process completed.")

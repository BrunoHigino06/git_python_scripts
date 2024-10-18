import boto3
from datetime import datetime
from botocore.exceptions import ClientError

# Configuration
SOURCE_BUCKET = 'thumbnail-on-demand'  # Replace with the source bucket name
DEST_BUCKET = 'thumbnail-image-poc'    # Replace with the destination bucket name

# Initialize S3 client (credentials are automatically loaded from CLI)
s3_client = boto3.client('s3')

# Function to copy XML files based on their last modified date
def copy_xml_files_by_modification_date(source_bucket, dest_bucket):
    print(f"Starting to copy XML files from {source_bucket} to {dest_bucket}...")

    try:
        # Use a paginator to list all objects in the source bucket
        paginator = s3_client.get_paginator('list_objects_v2')
        print(f"Listing files in bucket {source_bucket}...")

        # Iterate through all pages of objects
        for page in paginator.paginate(Bucket=source_bucket):
            if 'Contents' not in page:
                print(f"No files found in bucket {source_bucket}")
                return

            print(f"Files found in the current page: {len(page['Contents'])}")

            # Loop through each file in the current page
            for item in page['Contents']:
                file_key = item['Key']
                print(f"Processing file: {file_key}")

                # Check if the file is an XML file
                if file_key.endswith('.html'):
                    print(f"File {file_key} is an XML file, proceeding...")

                    # Get the last modified date of the file
                    last_modified = item['LastModified']
                    formatted_date = last_modified.strftime('%d-%m-%Y')  # Format date as dd-mm-yyyy
                    print(f"Last modified date: {formatted_date}")

                    # Define the destination path (directory based on the modification date) using forward slashes
                    destination_key = f"{formatted_date}/{file_key.split('/')[-1]}"
                    print(f"Preparing to copy file to directory {formatted_date} in bucket {dest_bucket}...")

                    # Copy the file to the destination bucket
                    try:
                        copy_source = {'Bucket': source_bucket, 'Key': file_key}
                        s3_client.copy(copy_source, dest_bucket, destination_key)
                        print(f"File {file_key} copied to {dest_bucket}/{destination_key}")
                    except ClientError as e:
                        print(f"Error copying {file_key}: {e}")
                else:
                    print(f"File {file_key} is not an XML file, skipping...")

    # Handle errors when listing files in the source bucket
    except ClientError as e:
        print(f"Error listing files in bucket {source_bucket}: {e}")

# Main script execution
if __name__ == '__main__':
    print("Starting the script to copy XML files between buckets...")
    copy_xml_files_by_modification_date(SOURCE_BUCKET, DEST_BUCKET)
    print("Process completed.")

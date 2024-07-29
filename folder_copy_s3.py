import boto3
import json

s3 = boto3.client('s3')
bucket_name = 'media-ingest-temporary'

def s3_report():
    all_folders = []
    continuation_token = None

    while True:
        if continuation_token:
            response = s3.list_objects_v2(
                Bucket=bucket_name,
                ContinuationToken=continuation_token
            )
        else:
            response = s3.list_objects_v2(Bucket=bucket_name)

        if 'Contents' in response:
            all_folders_objects = [
                {'Key': obj['Key'], 'LastModified': obj['LastModified'].isoformat()}
                for obj in response['Contents']
                if obj['Key'].endswith('Video/')
            ]
            all_folders.extend(all_folders_objects)

        if 'NextContinuationToken' in response:
            continuation_token = response['NextContinuationToken']
        else:
            break
    return json.dumps(all_folders, indent=4)

def create_folder_s3(folder_key):
    s3.put_object(Bucket=bucket_name, Key=folder_key)
    print(f"Create video folder in the path: {folder_key.replace('video/', '')} on the bucket: {bucket_name}")

def copy_content_s3(source_key, destination_key):
    if not source_key.endswith('/'):
        copy_source = {'Bucket': bucket_name, 'Key': source_key}
        print(f"Copy the {source_key} key to the new path {destination_key}")
        s3.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=destination_key)
        print(f"Copy the {source_key} key to the new path {destination_key} finish")

def folder_has_lowercase_video(folder_key):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_key.replace('Video/', 'video/'))
    for obj in response.get('Contents', []):
        if 'video/' in obj['Key']:
            return True
    return False

def process_folders():
    folders = json.loads(s3_report())
    
    for folder in folders:
        video_folder = folder['Key']
        new_folder = video_folder.replace('Video/', 'video/')
        
        if folder_has_lowercase_video(video_folder):
            print(f"Folder {video_folder} already has a lowercase 'video/' folder. Skipping...")
            continue
        
        create_folder_s3(new_folder)
        
        continuation_token = None
        while True:
            if continuation_token:
                response = s3.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=video_folder,
                    ContinuationToken=continuation_token
                )
            else:
                response = s3.list_objects_v2(
                    Bucket=bucket_name, 
                    Prefix=video_folder
                )
            
            if 'Contents' in response:
                for obj in response['Contents']:
                    source_key = obj['Key']
                    destination_key = source_key.replace('Video/', 'video/')
                    copy_content_s3(source_key, destination_key)
            
            if 'NextContinuationToken' in response:
                continuation_token = response['NextContinuationToken']
            else:
                break

# Execute the process
process_folders()

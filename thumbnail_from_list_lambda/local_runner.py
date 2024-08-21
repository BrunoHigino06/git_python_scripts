import boto3
import json

# Name of the source and destination buckets
source_bucket_name = 'avs-vod-mc-input-2c87c40d939653bdbef99ff1ce204afc'
destination_bucket_name = 'caracol-image-ingest-prod'

# Name of the Lambda function
lambda_function_name = 'GenerateThumbnails'

# Initialize S3 and Lambda clients
s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')

# List all .mp4 files in the source bucket
response = s3_client.list_objects_v2(Bucket=source_bucket_name)
mp4_files = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.mp4')]

# Function to check if a thumbnail already exists in the destination bucket
def thumbnail_exists(bucket_name, thumbnail_key):
    try:
        s3_client.head_object(Bucket=bucket_name, Key=thumbnail_key)
        return True
    except s3_client.exceptions.ClientError:
        return False

# Define the root directory for the thumbnails
sizes = [
    (260, 163, 'landscape-regular-thumb-mobile'),
    (377, 236, 'landscape-regular-thumb-tablet'),
    (426, 267, 'landscape-regular-thumb-tv')
]
format = 'jpg'
time_frames = ['00:05:00']

# Loop through each .mp4 file found
for video_key in mp4_files:
    print(f"Processing {video_key}...")
    
    root_dir = '/'.join(video_key.split('/')[:-2])
    
    all_thumbnails_exist = True
    for frame in time_frames:
        for width, height, name in sizes:
            thumbnail_key = f'{root_dir}/{name}.{format}'
            if not thumbnail_exists(destination_bucket_name, thumbnail_key):
                all_thumbnails_exist = False
                break
        if not all_thumbnails_exist:
            break

    if all_thumbnails_exist:
        print(f"All thumbnails for {video_key} already exist, skipping Lambda invocation.")
        continue

    # Invoke the Lambda function synchronously
    payload = json.dumps({
        'VIDEO_KEY': video_key,
        'SOURCE_BUCKET': source_bucket_name,
        'DESTINATION_BUCKET': destination_bucket_name
    })
    response = lambda_client.invoke(
        FunctionName=lambda_function_name,
        InvocationType='RequestResponse',  # Uses 'RequestResponse' for synchronous invocation
        Payload=payload  # Sends the video_key and bucket names as payload
    )

    # Check the Lambda response to ensure the execution was successful
    status_code = response['StatusCode']
    if status_code == 200:
        print(f"Lambda for {video_key} completed successfully.")
    else:
        print(f"Error invoking Lambda for {video_key}. Status code: {status_code}")

print("Processing completed.")

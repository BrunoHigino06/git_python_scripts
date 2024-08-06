import os
import boto3
import subprocess

# Explanation:
# This AWS Lambda function is triggered by an S3 event, which indicates that a new video file has been uploaded to an S3 bucket.
# The function performs the following steps:
# 1. Downloads the video file from the S3 bucket to the local temporary directory.
# 2. Generates thumbnails at specific time frames and resolutions using FFmpeg.
# 3. Uploads the generated thumbnails back to the root folder of the key in the S3 bucket.
# The thumbnails are named according to their resolution and include the mp4 file name:
# - 260x163: 'landscape-regular-thumb-mobile'
# - 377x236: 'landscape-regular-thumb-tablet'
# - 426x267: 'landscape-regular-thumb-tv'

# Initialize the S3 client
s3 = boto3.client('s3')

# Source and destination S3 bucket names
source_bucket_name = 'thumbnail-requeriments'
destination_bucket_name = 'thumbnail-requeriments'

# Time frames to capture thumbnails from the video
time_frames = ['00:10:00', '00:20:00', '00:30:00', '00:40:00', '00:50:00']

# Path to the FFmpeg binary
ffmpeg_path = "/opt/bin/ffmpeg"

# Thumbnail sizes and corresponding names
sizes = [
    (260, 163, 'landscape-regular-thumb-mobile'),
    (377, 236, 'landscape-regular-thumb-tablet'),
    (426, 267, 'landscape-regular-thumb-tv')
]

# Format for the thumbnail images
format = 'jpg'

# Function to download a video from S3
def s3_download(bucket_name, video_key, video_path):
    try:
        print(f'Downloading video: {video_key} from the S3 bucket: {bucket_name} to {video_path}')
        s3.download_file(bucket_name, video_key, video_path)
        if not os.path.exists(video_path):
            raise Exception(f'File {video_path} does not exist after download.')
    except Exception as e:
        print(f'Error downloading video from S3: {str(e)}')
        raise

# Function to create a thumbnail from the video
def create_thumbnail(video_path, frame, frame_thumbnail, width, height):
    try:
        ffmpeg_command = [
            ffmpeg_path, '-i', video_path, '-ss', frame, '-vframes', '1', 
            '-vf', f'scale={width}:{height}', f'{frame_thumbnail}.{format}'
        ]
        subprocess.run(ffmpeg_command, check=True)
        if not os.path.exists(f'{frame_thumbnail}.{format}'):
            raise Exception(f'Thumbnail {frame_thumbnail}.{format} was not created successfully.')
    except subprocess.CalledProcessError as e:
        print(f'FFmpeg error: {str(e)}')
        raise

# Function to upload a file to S3
def s3_upload(file_key, bucket_name, image_file):
    try:
        print(f'Uploading file {image_file} to {bucket_name} bucket on the path {file_key}')
        with open(image_file, 'rb') as f:
            s3.put_object(
                Bucket=bucket_name,
                Key=file_key,
                Body=f
            )
    except Exception as e:
        print(f'Error uploading thumbnail to S3: {str(e)}')
        raise

# Lambda function handler
def lambda_handler(event, context):
    for record in event['Records']:
        video_key = record['s3']['object']['key']
        local_video_path = f'/tmp/{os.path.basename(video_key)}'
        output_dir = '/tmp'

        try:
            print(f'Processing video: {video_key}')  # Additional logging
            # Download video from S3
            s3_download(source_bucket_name, video_key, local_video_path)
            
            # Parse the video key to get the desired root directory and video filename
            key_parts = video_key.split('/')
            print(f'Key Parts: {key_parts}')  # Debug statement to inspect the key parts
            
            if len(key_parts) < 5:
                raise Exception(f'Unexpected video key format: {video_key}')
            
            root_dir = '/'.join(key_parts[:-2])  # Root directory based on your key format
            video_filename = os.path.splitext(key_parts[-1])[0]  # Video filename without extension

            for frame in time_frames:
                for width, height, name in sizes:
                    frame_thumbnail = os.path.join(output_dir, f'{name}_{video_filename}_{frame.replace(":", "-")}')
                    create_thumbnail(local_video_path, frame, frame_thumbnail, width, height)
                    thumbnail_key = f'{root_dir}/{name}_{video_filename}_{frame.replace(":", "-")}.{format}'
                    s3_upload(thumbnail_key, destination_bucket_name, f'{frame_thumbnail}.{format}')

            # Clean up local video file
            os.remove(local_video_path)

        except Exception as e:
            print(f'Error in processing: {str(e)}')

    return {
        'statusCode': 200,
        'body': 'Thumbnails generated and uploaded successfully!'
    }

import os
import boto3
import subprocess
from datetime import datetime

# Initialize the S3 client
s3 = boto3.client('s3')

# Source and destination S3 bucket names
source_bucket_name = 'avs-vod-mc-input-2c87c40d939653bdbef99ff1ce204afc'
destination_bucket_name = 'caracol-image-ingest-prod'

# Path to the FFmpeg binary in CloudShell
ffmpeg_path = "/mnt/c/Users/Bruno/Desktop/clients/python_scripts/ffmpeg-7.0.1-amd64-static/ffmpeg"

# Thumbnail sizes and corresponding names
sizes = [
    (260, 163, 'landscape-regular-thumb-mobile'),
    (377, 236, 'landscape-regular-thumb-tablet'),
    (426, 267, 'landscape-regular-thumb-tv')
]

# Format for the thumbnail images
format = 'jpg'

# Time frames to capture thumbnails from the video
time_frames = ['00:05:00']

# Function to list all .mp4 files in the source bucket
def list_mp4_files(bucket_name):
    mp4_files = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.mp4'):
                mp4_files.append(obj['Key'])
    return mp4_files

# Function to check if a thumbnail already exists in the destination bucket
def thumbnail_exists(bucket_name, thumbnail_key):
    try:
        s3.head_object(Bucket=bucket_name, Key=thumbnail_key)
        return True
    except s3.exceptions.ClientError:
        return False

# Function to create a thumbnail from the video
def create_thumbnail(video_path, frame, frame_thumbnail, width, height):
    ffmpeg_command = [
        ffmpeg_path, '-i', video_path, '-ss', frame, '-vframes', '1', 
        '-vf', f'scale={width}:{height}', f'{frame_thumbnail}.{format}'
    ]
    subprocess.run(ffmpeg_command, check=True)

# Function to upload a thumbnail to the destination bucket
def s3_upload(file_key, bucket_name, image_file):
    print(f'Uploading file {image_file} to bucket {bucket_name} at path {file_key}')
    with open(image_file, 'rb') as f:
        s3.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=f
        )

# Main function to process videos in CloudShell
def process_videos():
    mp4_files = list_mp4_files(source_bucket_name)

    for video_key in mp4_files:
        video_filename = os.path.splitext(os.path.basename(video_key))[0]
        root_dir = os.path.dirname(os.path.dirname(video_key))  # Adjusted for directory structure
        output_dir = '/tmp'

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
            print(f'[{datetime.now()}] All thumbnails for {video_key} already exist, skipping download and generation.')
            continue

        # Log message before downloading the next video
        print(f'[{datetime.now()}] Starting download of video {video_key} from {source_bucket_name}...')

        local_video_path = f'/tmp/{os.path.basename(video_key)}'

        # Download the video from the source bucket
        s3.download_file(source_bucket_name, video_key, local_video_path)
        print(f'[{datetime.now()}] Finished downloading video {video_key}.')

        # Log message before starting the thumbnail generation
        print(f'[{datetime.now()}] Starting thumbnail generation for video {video_key}...')

        for frame in time_frames:
            for width, height, name in sizes:
                frame_thumbnail = os.path.join(output_dir, f'{name}')
                thumbnail_key = f'{root_dir}/{name}.{format}'

                if not thumbnail_exists(destination_bucket_name, thumbnail_key):
                    create_thumbnail(local_video_path, frame, frame_thumbnail, width, height)
                    s3_upload(thumbnail_key, destination_bucket_name, f'{frame_thumbnail}.{format}')
                    os.remove(f'{frame_thumbnail}.{format}')  # Remove the generated thumbnail after upload

        # Remove the local video file after processing
        os.remove(local_video_path)
        print(f'[{datetime.now()}] Finished processing video {video_key}.\n')

if __name__ == '__main__':
    process_videos()

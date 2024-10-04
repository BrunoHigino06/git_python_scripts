import os
import boto3
import subprocess

# Initialize the S3 client
s3 = boto3.client('s3')

# Source and destination S3 bucket names
source_bucket_name = 'avs-vod-mc-input-2c87c40d939653bdbef99ff1ce204afc'
destination_bucket_name = 'caracol-image-ingest-prod'

# Time frames to capture thumbnails from the video
time_frames = ['00:05:00']

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
    # List contents of /tmp at the start of the process
    print("Contents of /tmp before starting the process:")
    subprocess.run(['ls', '-lh', '/tmp'])
    
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
                    frame_thumbnail = os.path.join(output_dir, f'{name}')
                    create_thumbnail(local_video_path, frame, frame_thumbnail, width, height)
                    thumbnail_key = f'{root_dir}/{name}.{format}'
                    s3_upload(thumbnail_key, destination_bucket_name, f'{frame_thumbnail}.{format}')
                    
                    # List contents of /tmp before removing thumbnail
                    print("Contents of /tmp before removing thumbnail:")
                    subprocess.run(['ls', '-lh', '/tmp'])

                    # Clean up each thumbnail after upload
                    thumbnail_path = f'{frame_thumbnail}.{format}'
                    if os.path.exists(thumbnail_path):
                        os.remove(thumbnail_path)
                        print(f'Thumbnail {thumbnail_path} removed from /tmp/')  # Added print to signal deletion
                    
                    # List contents of /tmp after removing thumbnail
                    print("Contents of /tmp after removing thumbnail:")
                    subprocess.run(['ls', '-lh', '/tmp'])

            # List contents of /tmp before removing video
            print("Contents of /tmp before removing video:")
            subprocess.run(['ls', '-lh', '/tmp'])

            # Clean up local video file
            os.remove(local_video_path)
            print(f'Video {local_video_path} removed from /tmp/')  # Additional print for video removal

            # List contents of /tmp after removing video
            print("Contents of /tmp after removing video:")
            subprocess.run(['ls', '-lh', '/tmp'])

        except Exception as e:
            print(f'Error in processing: {str(e)}')

    return {
        'statusCode': 200,
        'body': 'Thumbnails generated, uploaded, and cleaned up successfully!'
    }

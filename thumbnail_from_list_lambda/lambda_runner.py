import boto3
import subprocess
import os
import json

# Initialize S3 client
s3 = boto3.client('s3')

# Thumbnail settings
sizes = [
    (260, 163, 'landscape-regular-thumb-mobile'),
    (377, 236, 'landscape-regular-thumb-tablet'),
    (426, 267, 'landscape-regular-thumb-tv')
]
format = 'jpg'
time_frames = ['00:05:00']  # Frames to capture thumbnails

# Path to FFmpeg
ffmpeg_path = '/opt/ffmpeg/ffmpeg'  # Adjust the path as necessary

def lambda_handler(event, context):
    # Extract the video_key from the event
    video_key = event.get('VIDEO_KEY')
    if not video_key:
        return {
            'statusCode': 400,
            'body': 'VIDEO_KEY not found in the event'
        }

    # Source and destination bucket names
    source_bucket_name = 'avs-vod-mc-input-2c87c40d939653bdbef99ff1ce204afc'
    destination_bucket_name = 'caracol-image-ingest-prod'

    # Temporary paths
    local_video_path = f'/tmp/{os.path.basename(video_key)}'
    
    try:
        # Download the video from the S3 bucket
        s3.download_file(source_bucket_name, video_key, local_video_path)
        
        # Create thumbnails
        root_dir = '/'.join(video_key.split('/')[:-2])  # Extracts the root directory
        
        for frame in time_frames:
            for width, height, name in sizes:
                frame_thumbnail = f'/tmp/{name}.{format}'
                thumbnail_key = f'{root_dir}/{name}.{format}'
                
                # Generate the thumbnail
                ffmpeg_command = [
                    ffmpeg_path, '-i', local_video_path, '-ss', frame, '-vframes', '1', 
                    '-vf', f'scale={width}:{height}', frame_thumbnail
                ]
                subprocess.run(ffmpeg_command, check=True)
                
                # Upload the thumbnail to the S3 bucket
                with open(frame_thumbnail, 'rb') as f:
                    s3.put_object(
                        Bucket=destination_bucket_name,
                        Key=thumbnail_key,
                        Body=f
                    )
                
                # Remove the local thumbnail after upload
                os.remove(frame_thumbnail)

        # Remove the local video file after processing
        os.remove(local_video_path)
        
        return {
            'statusCode': 200,
            'body': 'Thumbnails generated and uploaded successfully.'
        }
    
    except Exception as e:
        print(f'Error: {e}')
        return {
            'statusCode': 500,
            'body': f'Error processing video: {e}'
        }

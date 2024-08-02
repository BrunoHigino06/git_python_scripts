import os
import boto3
import subprocess

s3 = boto3.client('s3')
source_bucket_name = 'thumbnail-requeriments'
destination_bucket_name = 'thumbnail-requeriments'
time_frames = ['00:10:00', '00:20:00', '00:30:00', '00:40:00', '00:50:00']
ffmpeg_path = "/opt/bin/ffmpeg"

sizes = [
    (260, 163),
    (377, 236),
    (426, 267)
]
format = 'jpg'

def s3_download(bucket_name, video_key, video_path):
    try:
        print(f'Downloading video: {video_key} from the S3 bucket: {bucket_name} to {video_path}')
        s3.download_file(bucket_name, video_key, video_path)
        if not os.path.exists(video_path):
            raise Exception(f'File {video_path} does not exist after download.')
    except Exception as e:
        print(f'Error downloading video from S3: {str(e)}')
        raise

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

def lambda_handler(event, context):
    for record in event['Records']:
        video_key = record['s3']['object']['key']
        local_video_path = f'/tmp/{os.path.basename(video_key)}'
        output_dir = '/tmp'

        try:
            print(f'Processing video: {video_key}')  # Additional logging
            # Download video from S3
            s3_download(source_bucket_name, video_key, local_video_path)
            
            # Parse the video key to get the desired directory structure
            key_parts = video_key.split('/')
            print(f'Key Parts: {key_parts}')  # Debug statement to inspect the key parts
            
            if len(key_parts) < 3:
                raise Exception(f'Unexpected video key format: {video_key}')
            
            base_dir = key_parts[0]  # Adjusted based on your key format
            video_id = key_parts[1].split('.')[0]  # Adjusted based on your key format

            for frame in time_frames:
                for width, height in sizes:
                    frame_thumbnail = os.path.join(output_dir, f'image_{frame.replace(":", "-")}_{width}x{height}')
                    create_thumbnail(local_video_path, frame, frame_thumbnail, width, height)
                    thumbnail_key = f'{base_dir}/{video_id}/image_{frame.replace(":", "-")}_{width}x{height}.{format}'
                    s3_upload(thumbnail_key, destination_bucket_name, f'{frame_thumbnail}.{format}')

            # Clean up EFS after processing
            os.remove(local_video_path)

        except Exception as e:
            print(f'Error in processing: {str(e)}')

    return {
        'statusCode': 200,
        'body': 'Thumbnails generated and uploaded successfully!'
    }

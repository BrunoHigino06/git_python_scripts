import os
import boto3
import subprocess

s3 = boto3.client('s3')
bucket_name = 'thumbnail-generator-poc'
local_video_path = '/tmp/video.mp4'
video_key = '100000/video/1005010002.mp4'
time_frames = ['00:05:00', '00:10:00', '00:15:00', '00:20:00', '00:25:00', '00:30:00', '00:35:00', '00:40:00', '00:45:00', '00:50:00', '00:55:00', '01:00:00']

# Garantir que o diretório de saída exista
output_dir = './tmp'
os.makedirs(output_dir, exist_ok=True)

def s3_download(bucket_name, video_key, video_path):
    try:
        print(f'Downloading video: {video_key} from the S3 bucket: {bucket_name} to {video_path}')
        s3.download_file(bucket_name, video_key, video_path)

        if not os.path.exists(video_path):
            raise Exception(f'File {video_path} does not exist after download.')
    except Exception as e:
        print(f'Error downloading video from S3: {str(e)}')
        raise

def create_thumbnail(video_path, frame, frame_thumbnail):
    ffmpeg_command = [
        'ffmpeg', '-i', video_path, '-ss', frame, '-vsync', 'vfr', '-frames:v', '1', frame_thumbnail
    ]
    result = subprocess.run(ffmpeg_command, capture_output=True, text=True)
    print(f'FFmpeg command output: {result.stdout}')
    print(f'FFmpeg command error: {result.stderr}')

    if not os.path.exists(frame_thumbnail):
        raise Exception(f'Thumbnail {frame_thumbnail} was not created successfully.')

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

try:
    s3_download(bucket_name, video_key, local_video_path)
    
    for frame in time_frames:
        frame_thumbnail = os.path.join(output_dir, f'image_{frame.replace(":", "-")}.png')
        create_thumbnail(local_video_path, frame, frame_thumbnail)
        s3_upload(f'thumbnails/{os.path.basename(frame_thumbnail)}', bucket_name, frame_thumbnail)

except Exception as e:
    print(f'Error in processing: {str(e)}')

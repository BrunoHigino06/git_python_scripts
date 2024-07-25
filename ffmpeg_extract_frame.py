import boto3, subprocess

s3 = boto3.client('s3')
bucket_name = 'thumbnail-generator-poc'
local_video_path = '/tmp/video.mp4'
video_key = '100000/video/1005010002.mp4'
time_frames = ['00:05:00', '00:10:00', '00:15:00', '00:20:00', '00:25:00', '00:30:00', '00:35:00', '00:40:00', '00:45:00', '00:50:00', '00:55:00', '01:00:00']


def s3_download(bucket_name, video_key, video_path):

    try:

        print(f'Downloading video: {video_key} from the S3 bucket: {bucket_name} to {video_path}')
        s3.download_file(bucket_name, video_key, video_path)
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

def s3_upload(file_key, bucket_name, image_file):

    try:

        print(f'Uploading file {image_file} to {bucket_name} bucket on the path {file_key}')
        s3.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=image_file
        )

    except Exception as e:
        print(f'Error upload thumbnail to S3: {str(e)}')
        raise

s3_download(bucket_name, video_key, local_video_path)

for frame in time_frames:
    frame_thumbnail = f'./tmp/image_{frame}.png'
    s3_image_location = video_key.replace('1005010002.mp4', '')
    create_thumbnail(local_video_path, frame, frame_thumbnail)
    s3_upload(s3_image_location, bucket_name, frame_thumbnail)
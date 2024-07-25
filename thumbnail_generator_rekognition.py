"""
This script demonstrates how to use AWS Rekognition to detect labels in a video stored in S3,
extract key frames based on certain labels (e.g., Person, Face, Object, Scene), and save those frames locally.

It performs the following steps:
1. Downloads the video from S3 to a local directory.
2. Initiates label detection analysis on the video using AWS Rekognition.
3. Monitors the status of the job until it completes.
4. Extracts key frames where labels of interest are detected.
5. Uses FFmpeg to extract and save these key frames as PNG images locally.

Dependencies:
- boto3: AWS SDK for Python
- FFmpeg: Multimedia framework for handling video, audio, and other multimedia files

Ensure you have FFmpeg installed and accessible in '/usr/local/bin/ffmpeg/ffmpeg-7.0.1-amd64-static/ffmpeg'.

Note: Adjust the S3 bucket name ('bucket') and video file name ('video_key') as per your setup.
"""

import boto3
import time
import subprocess
import os

# AWS Rekognition and S3 clients
rekognition = boto3.client('rekognition')
s3 = boto3.client('s3')

# Function to start video analysis
def start_video_analysis(bucket, video):
    """
    Starts label detection analysis on the specified video in S3.
    
    Args:
    - bucket (str): S3 bucket name where the video is stored.
    - video (str): Key name of the video file in the S3 bucket.
    
    Returns:
    - job_id (str): ID of the analysis job started on AWS Rekognition.
    """
    response = rekognition.start_label_detection(
        Video={'S3Object': {'Bucket': bucket, 'Name': video}}
    )
    job_id = response['JobId']
    print(f'Started video analysis, JobId: {job_id}')
    return job_id

# Function to check job status
def check_job_status(job_id):
    """
    Checks the status of the label detection job on AWS Rekognition.
    Waits until the job completes.
    
    Args:
    - job_id (str): ID of the analysis job on AWS Rekognition.
    
    Returns:
    - status (dict): Dictionary containing job status information.
    """
    status = rekognition.get_label_detection(JobId=job_id)
    while status['JobStatus'] == 'IN_PROGRESS':
        print("Waiting for analysis to complete...")
        time.sleep(10)
        status = rekognition.get_label_detection(JobId=job_id)
    print(f'Job status: {status}')
    return status

# Function to extract key frames
def extract_key_frames(job_id):
    """
    Extracts key frames from the video where labels of interest (e.g., Person, Face, Object, Scene) are detected.
    
    Args:
    - job_id (str): ID of the analysis job on AWS Rekognition.
    
    Returns:
    - key_frames (list): List of timestamps (in milliseconds) where key frames are detected.
    """
    results = rekognition.get_label_detection(JobId=job_id)
    key_frames = []

    if results['JobStatus'] == 'SUCCEEDED':
        for label in results['Labels']:
            if label['Label']['Name'] in ['Person', 'Face', 'Object', 'Scene']:  # Adjust as needed
                timestamp = label['Timestamp']
                key_frames.append(timestamp)

    print(f'Extracted key frames: {key_frames}')
    return key_frames

# Function to download video from S3
def download_video_from_s3(bucket, video_key, local_path):
    """
    Downloads the video file from S3 to a local directory.
    
    Args:
    - bucket (str): S3 bucket name where the video is stored.
    - video_key (str): Key name of the video file in the S3 bucket.
    - local_path (str): Local path where the video file will be downloaded.
    """
    try:
        s3.download_file(bucket, video_key, local_path)
        print(f'Downloaded video {video_key} from S3 to {local_path}')
    except Exception as e:
        print(f'Error downloading video from S3: {str(e)}')
        raise

# Function to save frames locally
def save_frame_locally(video_path, timestamp):
    """
    Extracts and saves a key frame from the video locally as a PNG image file.
    
    Args:
    - video_path (str): Local path of the video file.
    - timestamp (int): Timestamp (in milliseconds) of the key frame to extract.
    
    Returns:
    - frame_file (str): Local path where the frame image file is saved.
    """
    # Create the ./frames directory if it doesn't exist
    os.makedirs('./frames', exist_ok=True)

    frame_file = f'./frames/frame_{timestamp}.png'  # Define the output file path

    # FFmpeg command to extract the frame
    ffmpeg_command = [
        'ffmpeg', '-i', video_path, '-vf', f'select=eq(n\,{timestamp})', '-vsync', 'vfr', frame_file
    ]

    # Execute the FFmpeg command
    result = subprocess.run(ffmpeg_command, capture_output=True, text=True)
    print(f'FFmpeg command output: {result.stdout}')
    print(f'FFmpeg command error: {result.stderr}')

    # Check if the file was generated
    if not os.path.exists(frame_file):
        print(f'Error: Frame {timestamp} was not generated.')
        return

    print(f'Saved frame {timestamp} locally at {frame_file}')
    return frame_file

# S3 parameters
bucket = 'thumbnail-generator-poc'
video_key = 'file_example_MP4_1920_18MG.mp4'  # Adjust as per your setup
local_video_path = '/tmp/video.mp4'  # Temporary path to save the video locally

# Download the video from S3
download_video_from_s3(bucket, video_key, local_video_path)

# Start video analysis
job_id = start_video_analysis(bucket, video_key)

# Check job status
status = check_job_status(job_id)

# Extract key frames
key_frames = extract_key_frames(job_id)

# Save key frames locally
for timestamp in key_frames:
    save_frame_locally(local_video_path, timestamp)

print("Key frames saved locally.")

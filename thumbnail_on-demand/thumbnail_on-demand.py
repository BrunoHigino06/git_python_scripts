import os
import boto3
import subprocess
import json

# Variáveis de input configuráveis no topo
SOURCE_BUCKET_NAME = '<your-source-bucket>'  # Nome do bucket de origem do arquivo .mp4
VIDEO_KEY = '<path/to/your/video.mp4>'  # Caminho do arquivo .mp4 no bucket de origem
DESTINATION_BUCKET_NAME = '<your-destination-bucket>'  # Nome do bucket de destino para os thumbnails
LOG_BUCKET = '<your-log-bucket>'  # Nome do bucket onde os logs serão armazenados
FFMPEG_PATH = "/opt/bin/ffmpeg"  # Caminho do FFmpeg
TIME_FRAMES = ['00:05:00']  # Lista de frames de tempo para gerar os thumbnails
SIZES = [  # Tamanhos de thumbnails
    (260, 163, 'landscape-regular-thumb-mobile'),
    (377, 236, 'landscape-regular-thumb-tablet'),
    (426, 267, 'landscape-regular-thumb-tv')
]
FORMAT = 'jpg'  # Formato das thumbnails

# Inicializa o cliente S3
s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')

def write_logs_to_s3(logs, job_id):
    """Escreve os logs no bucket de logs no S3"""
    s3.put_object(
        Bucket=LOG_BUCKET,
        Key=f'logs/{job_id}.json',
        Body=json.dumps(logs),
        ContentType='application/json'
    )

def s3_download(bucket_name, video_key, video_path, logs, job_id):
    """Faz download do arquivo de vídeo do S3"""
    try:
        log_message = f'Downloading video: {video_key} from the S3 bucket: {bucket_name}'
        logs.append(log_message)
        write_logs_to_s3(logs, job_id)
        
        s3.download_file(bucket_name, video_key, video_path)
        if not os.path.exists(video_path):
            raise Exception(f'File {video_path} does not exist after download.')
    except Exception as e:
        logs.append(f'Error downloading video from S3: {str(e)}')
        write_logs_to_s3(logs, job_id)
        raise

def create_thumbnail(video_path, frame, frame_thumbnail, width, height, logs, job_id):
    """Cria thumbnails a partir de um frame específico"""
    try:
        logs.append(f'Generating thumbnail at {frame} with size {width}x{height}')
        write_logs_to_s3(logs, job_id)
        
        ffmpeg_command = [
            FFMPEG_PATH, '-i', video_path, '-ss', frame, '-vframes', '1', 
            '-vf', f'scale={width}:{height}', f'{frame_thumbnail}.{FORMAT}'
        ]
        subprocess.run(ffmpeg_command, check=True)
        if not os.path.exists(f'{frame_thumbnail}.{FORMAT}'):
            raise Exception(f'Thumbnail {frame_thumbnail}.{FORMAT} was not created successfully.')
    except subprocess.CalledProcessError as e:
        logs.append(f'FFmpeg error: {str(e)}')
        write_logs_to_s3(logs, job_id)
        raise

def s3_upload(file_key, bucket_name, image_file, logs, job_id):
    """Faz upload dos thumbnails gerados para o S3"""
    try:
        logs.append(f'Uploading thumbnail to S3: {file_key}')
        write_logs_to_s3(logs, job_id)
        
        with open(image_file, 'rb') as f:
            s3.put_object(
                Bucket=bucket_name,
                Key=file_key,
                Body=f,
                ContentType='image/jpeg'
            )
    except Exception as e:
        logs.append(f'Error uploading thumbnail to S3: {str(e)}')
        write_logs_to_s3(logs, job_id)
        raise

def process_video(source_bucket_name, video_key, destination_bucket_name, logs, job_id):
    """Processa o vídeo e gera thumbnails"""
    local_video_path = f'/tmp/{os.path.basename(video_key)}'
    output_dir = '/tmp'
    thumbnail_urls = []

    try:
        logs.append(f'Processing video: {video_key}')
        write_logs_to_s3(logs, job_id)
        
        # Faz download do vídeo
        s3_download(source_bucket_name, video_key, local_video_path, logs, job_id)
        
        # Divide o caminho do arquivo para formar o caminho de destino dos thumbnails
        key_parts = video_key.split('/')
        root_dir = '/'.join(key_parts[:-2])  # Exemplo: 10020000/10020100/10020101
        video_filename = os.path.splitext(key_parts[-1])[0]

        # Para cada frame de tempo e tamanho, gera os thumbnails
        for frame in TIME_FRAMES:
            for width, height, name in SIZES:
                frame_thumbnail = os.path.join(output_dir, f'{name}_{video_filename}_{frame}')
                create_thumbnail(local_video_path, frame, frame_thumbnail, width, height, logs, job_id)

                file_key = f'{root_dir}/{name}/{video_filename}_{frame}.{FORMAT}'
                s3_upload(file_key, destination_bucket_name, f'{frame_thumbnail}.{FORMAT}', logs, job_id)

                thumbnail_urls.append(f's3://{destination_bucket_name}/{file_key}')

    except Exception as e:
        logs.append(f'Error during video processing: {str(e)}')
        write_logs_to_s3(logs, job_id)
        raise
    
    return thumbnail_urls

def lambda_handler(event, context):
    # O Lambda agora usa as variáveis definidas no topo
    source_bucket_name = SOURCE_BUCKET_NAME
    video_key = VIDEO_KEY
    destination_bucket_name = DESTINATION_BUCKET_NAME

    job_id = context.aws_request_id
    logs = []

    try:
        logs.append('Started thumbnail generation process.')
        write_logs_to_s3(logs, job_id)
        
        # Processa o vídeo e gera thumbnails
        thumbnail_urls = process_video(source_bucket_name, video_key, destination_bucket_name, logs, job_id)
        
        logs.append('Thumbnail generation completed.')
        write_logs_to_s3(logs, job_id)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'jobId': job_id,
                'thumbnails': thumbnail_urls,
                'logs': logs
            })
        }
    except Exception as e:
        logs.append(f'Error: {str(e)}')
        write_logs_to_s3(logs, job_id)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'An error occurred.',
                'error': str(e),
                'logs': logs
            })
        }
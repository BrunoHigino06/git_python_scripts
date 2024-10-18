import os
import boto3
import subprocess
import json

# Inicializa o cliente S3
s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')

# Defina o bucket de logs como uma variável fixa
LOG_BUCKET = 'thumbnail-on-demand'

# Configurações de tamanho para thumbnails
SIZES = [
    (260, 163, 'landscape-regular-thumb-mobile'),
    (377, 236, 'landscape-regular-thumb-tablet'),
    (426, 267, 'landscape-regular-thumb-tv')
]

# Formato de saída das thumbnails
FORMAT = 'jpg'

def cleanup_tmp():
    """Remove todos os arquivos do diretório /tmp"""
    try:
        for filename in os.listdir('/tmp'):
            file_path = os.path.join('/tmp', filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
    except Exception as e:
        print(f'Erro ao limpar o diretório /tmp: {str(e)}')

def write_logs_to_s3(logs, job_id):
    """Escreve os logs no bucket de logs no S3"""
    try:
        s3.put_object(
            Bucket=LOG_BUCKET,
            Key=f'logs/{job_id}.json',
            Body=json.dumps(logs),
            ContentType='application/json'
        )
    except Exception as e:
        print(f'Erro ao escrever logs no S3: {str(e)}')

def download_video_segment(bucket_name, video_key, start_time, duration, local_video_path, logs, job_id):
    """Baixa um segmento do vídeo do S3 usando FFmpeg"""
    try:
        log_message = f'Downloading video segment: {video_key} from the S3 bucket: {bucket_name} starting at {start_time} for {duration} seconds'
        logs.append(log_message)
        write_logs_to_s3(logs, job_id)

        # Baixar o vídeo do S3 para o diretório EFS
        s3.download_file(bucket_name, video_key, local_video_path)
        
        # Comando FFmpeg para baixar o segmento do vídeo
        ffmpeg_command = [
            'ffmpeg', '-i', local_video_path, '-ss', str(start_time), 
            '-t', str(duration), '-c', 'copy', local_video_path
        ]
        subprocess.run(ffmpeg_command, check=True)
        if not os.path.exists(local_video_path):
            raise FileNotFoundError(f'File {local_video_path} does not exist after download.')
    except subprocess.CalledProcessError as e:
        logs.append(f'FFmpeg error: {str(e)}')
        write_logs_to_s3(logs, job_id)
        raise
    except Exception as e:
        logs.append(f'Error downloading video segment from S3: {str(e)}')
        write_logs_to_s3(logs, job_id)
        raise

def create_thumbnail(video_path, frame, frame_thumbnail, width, height, logs, job_id):
    """Cria thumbnails a partir de um frame específico"""
    try:
        logs.append(f'Generating thumbnail at {frame} with size {width}x{height}')
        write_logs_to_s3(logs, job_id)
        
        ffmpeg_command = [
            'ffmpeg', '-i', video_path, '-ss', frame, '-vframes', '1', 
            '-vf', f'scale={width}:{height}', frame_thumbnail
        ]
        subprocess.run(ffmpeg_command, check=True)
        if not os.path.exists(frame_thumbnail):
            raise Exception(f'Thumbnail {frame_thumbnail} was not created successfully.')
    except subprocess.CalledProcessError as e:
        logs.append(f'FFmpeg error: {str(e)}')
        write_logs_to_s3(logs, job_id)
        raise
    except Exception as e:
        logs.append(f'Error creating thumbnail: {str(e)}')
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

def process_video(source_bucket_name, video_key, destination_bucket_name, time_frames, logs, job_id):
    """Processa o vídeo e gera thumbnails"""
    local_video_path = f'/tmp/{os.path.basename(video_key)}'  # Usando o EFS em vez de /tmp
    output_dir = '/tmp'  # Usando o EFS
    thumbnail_urls = []
    
    # Defina o tempo inicial e a duração do segmento do vídeo que você quer baixar
    START_TIME = 0  # Por exemplo, inicie do início
    DURATION = 10  # Duração de 10 segundos

    try:
        logs.append(f'Processing video: {video_key}')
        write_logs_to_s3(logs, job_id)
        
        # Faz download de um segmento do vídeo
        download_video_segment(source_bucket_name, video_key, START_TIME, DURATION, local_video_path, logs, job_id)
        
        # Divide o caminho do arquivo para formar o caminho de destino dos thumbnails
        key_parts = video_key.split('/')
        root_dir = '/'.join(key_parts[:-2])  # Exemplo: 1012000000/1012010000/1012010001
        video_filename = os.path.splitext(key_parts[-1])[0]

        # Para cada frame de tempo e tamanho, gera os thumbnails
        for frame in time_frames:
            for width, height, name in SIZES:
                # Mude o caminho do thumbnail para usar o nome correto
                frame_thumbnail = os.path.join(output_dir, f'{name}.{FORMAT}')
                
                # Gera a thumbnail
                create_thumbnail(local_video_path, frame, frame_thumbnail, width, height, logs, job_id)

                # Ajusta o file_key para que as imagens sejam armazenadas no diretório do vídeo
                file_key = f'{root_dir}/{name}.{FORMAT}'  # Ajusta para o nome desejado
                s3_upload(file_key, destination_bucket_name, frame_thumbnail, logs, job_id)

                thumbnail_urls.append(f's3://{destination_bucket_name}/{file_key}')

    except Exception as e:
        logs.append(f'Error during video processing: {str(e)}')
        write_logs_to_s3(logs, job_id)
        raise
    
    return thumbnail_urls

def lambda_handler(event, context):
    """AWS Lambda function handler."""
    logs = []  # Inicializar os logs
    try:

        cleanup_tmp()
        # Analisar o corpo do evento
        body = json.loads(event['body'])
        source_bucket_name = body['source_bucket']
        video_key = body['video_key']
        destination_bucket_name = body['destination_bucket']
        time_frames = body['time_frames']

        job_id = context.aws_request_id
        logs.append(f'Received request: {json.dumps(body)}')

        write_logs_to_s3(logs, job_id)

        thumbnail_urls = process_video(source_bucket_name, video_key, destination_bucket_name, time_frames, logs, job_id)

        # Construir o link para os logs do CloudWatch
        log_url = f'https://console.aws.amazon.com/cloudwatch/home?region={context.invoked_function_arn.split(":")[3]}#logsV2:log-groups/log-group:/aws/lambda/{context.function_name}/log-events/{job_id}'

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Thumbnail generation process completed!',
                'logs': logs,
                'thumbnails': thumbnail_urls,
                'log_url': log_url  # Adicionando o link para o CloudWatch
            })
        }
    except Exception as e:
        error_message = f'Error processing request: {str(e)}'
        logs.append(error_message)
        write_logs_to_s3(logs, context.aws_request_id)

        # Construir o link para os logs do CloudWatch em caso de erro
        log_url = f'https://console.aws.amazon.com/cloudwatch/home?region={context.invoked_function_arn.split(":")[3]}#logsV2:log-groups/log-group:/aws/lambda/{context.function_name}/log-events/{context.aws_request_id}'

        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': error_message,
                'logs': logs,
                'log_url': log_url  # Adicionando o link para o CloudWatch
            })
        }

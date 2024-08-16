import boto3

def count_mp4_files(bucket_name):
    # Inicializa o cliente S3
    s3 = boto3.client('s3')
    
    # Conta de arquivos .mp4
    mp4_count = 0
    
    # Pagina através de todos os objetos no bucket
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name)
    
    for page in page_iterator:
        if 'Contents' in page:
            for item in page['Contents']:
                if item['Key'].endswith('.mp4'):
                    mp4_count += 1
    
    return mp4_count

# Nome do bucket
bucket_name = 'avs-vod-mc-input-2c87c40d939653bdbef99ff1ce204afc'

# Conta arquivos .mp4
count = count_mp4_files(bucket_name)
print(f"Total de arquivos .mp4 no bucket '{bucket_name}': {count}")

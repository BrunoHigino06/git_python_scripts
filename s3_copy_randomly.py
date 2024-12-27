import boto3
import random
from botocore.exceptions import NoCredentialsError

def list_objects(s3_client, bucket_name, prefix):
    """Lista todos os objetos em um bucket com o prefixo fornecido."""
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    objects = []
    for page in pages:
        objects.extend(page.get('Contents', []))
    return objects

def copy_random_files(source_bucket, target_bucket, prefix, s3_client, num_files=5):
    """Copia aleatoriamente `num_files` arquivos do terceiro nível de diretórios para outro bucket."""
    level_2_prefixes = set()
    
    # Lista objetos no bucket origem
    objects = list_objects(s3_client, source_bucket, prefix)

    # Encontra os prefixos de nível 2
    for obj in objects:
        key_parts = obj['Key'].split('/')
        if len(key_parts) >= 3:
            level_2_prefixes.add(f"{key_parts[0]}/{key_parts[1]}")

    for level_2_prefix in level_2_prefixes:
        # Lista diretórios de nível 3
        level_3_dirs = set()
        for obj in objects:
            if obj['Key'].startswith(level_2_prefix):
                key_parts = obj['Key'].split('/')
                if len(key_parts) >= 4:
                    level_3_dirs.add(f"{key_parts[0]}/{key_parts[1]}/{key_parts[2]}")

        level_3_dirs = list(level_3_dirs)
        if len(level_3_dirs) < num_files:
            print(f"Aviso: Apenas {len(level_3_dirs)} diretórios disponíveis em {level_2_prefix}, menos que o necessário ({num_files}).")

        selected_dirs = random.sample(level_3_dirs, min(len(level_3_dirs), num_files))

        for dir_prefix in selected_dirs:
            # Copia arquivos .mp4 do diretório selecionado
            for obj in objects:
                if obj['Key'].startswith(dir_prefix) and obj['Key'].endswith('.mp4'):
                    source_key = obj['Key']
                    target_key = obj['Key']

                    # Verifica se já existe no bucket destino
                    try:
                        s3_client.head_object(Bucket=target_bucket, Key=target_key)
                        print(f"Arquivo já existe no destino: {target_key}, pulando.")
                        continue
                    except:
                        pass

                    # Copia o arquivo
                    copy_source = {'Bucket': source_bucket, 'Key': source_key}
                    s3_client.copy(copy_source, target_bucket, target_key)
                    print(f"Arquivo copiado: {source_key} para {target_key}")

if __name__ == "__main__":
    source_bucket = "random-copy-23-12-2024"
    target_bucket = "random-copy-poc-23-12-2024-output"
    prefix = ""  # Prefixo inicial, caso queira filtrar

    try:
        s3_client = boto3.client('s3')
        copy_random_files(source_bucket, target_bucket, prefix, s3_client, num_files=5)
    except NoCredentialsError:
        print("Credenciais AWS não encontradas. Certifique-se de configurá-las corretamente.")
    except Exception as e:
        print(f"Erro ao executar o script: {e}")

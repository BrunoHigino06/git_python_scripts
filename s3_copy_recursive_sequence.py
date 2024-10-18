import subprocess

def copy_s3_files(source_account_id, dest_account_id, source_bucket, source_path, dest_bucket, dest_path, start_dir, num_dirs):
    """
    Copia os diretórios de um bucket S3 de uma conta para outra, começando a partir de um diretório especificado e copiando um número definido de diretórios.

    :param source_account_id: ID da conta de origem.
    :param dest_account_id: ID da conta de destino.
    :param source_bucket: Nome do bucket de origem.
    :param source_path: Caminho no bucket de origem.
    :param dest_bucket: Nome do bucket de destino.
    :param dest_path: Caminho no bucket de destino.
    :param start_dir: Diretório inicial a partir do qual copiar.
    :param num_dirs: Número máximo de diretórios a copiar (pode ser maior que os disponíveis).
    """
    count = 0
    current_dir = int(start_dir)

    # Caminho completo do diretório de origem no formato S3
    source_prefix = f"s3://{source_bucket}/{source_path}"
    print(f"Listando conteúdo do bucket de origem: {source_prefix}")

    while count < num_dirs:
        source_dir = f"{source_prefix}{current_dir}/"
        dest_dir = f"s3://{dest_bucket}/{dest_path}/{current_dir}/"
        
        # Comando AWS CLI para verificar se o diretório existe
        check_command = f"aws s3 ls {source_dir}"
        check_result = subprocess.run(check_command, shell=True, capture_output=True, text=True)

        # Se o diretório existe, faz a cópia
        if check_result.returncode == 0:
            # Comando AWS CLI para copiar o diretório
            copy_command = f"aws s3 cp {source_dir} {dest_dir} --recursive"
            print(f"Executando cópia: {copy_command}")
            
            # Executa o comando de cópia
            copy_result = subprocess.run(copy_command, shell=True, capture_output=True, text=True)
            
            # Verifica se houve erro na cópia
            if copy_result.returncode != 0:
                print(f"Erro ao copiar o diretório {current_dir}: {copy_result.stderr}")
            else:
                print(f"Diretório copiado com sucesso: {current_dir}")
                
            count += 1  # Incrementa o contador de diretórios copiados
        else:
            print(f"O diretório {current_dir} não existe. Pulando para o próximo diretório.")

        current_dir += 1  # Passa para o próximo diretório

    if count == 0:
        print("Nenhum diretório foi encontrado para copiar.")

# Variáveis configuráveis
source_account_id = "851725566176"  # ID da conta de origem
dest_account_id = "851725566176"    # ID da conta de destino
source_bucket = "media-ingest-temporary"  # Bucket de origem
source_path = "1029000000/1029020000/"    # Caminho de origem
dest_bucket = "avs-vod-mc-input-2c87c40d939653bdbef99ff1ce204afc"  # Bucket de destino
dest_path = "1029000000/1029020000"  # Caminho de destino

# Diretório inicial e número de diretórios a copiar
start_dir = "1029020384"
num_dirs = 30  # Exemplo de valor maior que o número de diretórios disponíveis

# Chama a função para executar a cópia
copy_s3_files(source_account_id, dest_account_id, source_bucket, source_path, dest_bucket, dest_path, start_dir, num_dirs)

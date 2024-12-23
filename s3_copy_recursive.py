import subprocess

def copy_s3_files(source_account_id, dest_account_id, source_bucket, source_path, dest_bucket, dest_path, start_dir, end_dir):
    """
    Copia os diretórios de um bucket S3 de uma conta para outra dentro de um intervalo especificado.

    :param source_account_id: ID da conta de origem.
    :param dest_account_id: ID da conta de destino.
    :param source_bucket: Nome do bucket de origem.
    :param source_path: Caminho no bucket de origem.
    :param dest_bucket: Nome do bucket de destino.
    :param dest_path: Caminho no bucket de destino.
    :param start_dir: Diretório inicial no intervalo.
    :param end_dir: Diretório final no intervalo.
    """
    count = 0

    # Caminho completo do diretório de origem no formato S3
    source_prefix = f"s3://{source_bucket}/{source_path}"
    print(f"Listando conteúdo do bucket de origem: {source_prefix}")

    # Comando AWS CLI para listar os diretórios
    list_command = f"aws s3 ls {source_prefix}"
    
    try:
        # Executa o comando de listagem
        result = subprocess.run(list_command, shell=True, capture_output=True, text=True)
        
        # Verifica se houve erro na listagem
        if result.returncode != 0:
            print(f"Erro ao listar diretórios: {result.stderr}")
            return
        
        directories = result.stdout.splitlines()

        # Iterar sobre os diretórios e copiar aqueles que estão no intervalo especificado
        for directory in directories:
            # Extrair o nome do diretório (espera-se que ele termine com '/')
            dir_name = directory.split()[-1].strip('/')

            # Verificar se o diretório está dentro do intervalo especificado
            if start_dir <= dir_name <= end_dir:
                # Caminhos completos S3 para origem e destino
                source_dir = f"{source_prefix}{dir_name}/"
                dest_dir = f"s3://{dest_bucket}/{dest_path}/{dir_name}/"
                
                # Comando AWS CLI para copiar o diretório
                copy_command = f"aws s3 cp {source_dir} {dest_dir} --recursive"
                print(f"Executando cópia: {copy_command}")
                
                # Executa o comando de cópia
                copy_result = subprocess.run(copy_command, shell=True, capture_output=True, text=True)
                
                # Verifica se houve erro na cópia
                if copy_result.returncode != 0:
                    print(f"Erro ao copiar o diretório {dir_name}: {copy_result.stderr}")
                else:
                    print(f"Diretório copiado com sucesso: {dir_name}")
                
                # Incrementa o contador
                count += 1

        if count == 0:
            print("Nenhum diretório foi encontrado no intervalo especificado.")
    
    except Exception as e:
        print(f"Erro inesperado: {e}")

# Variáveis configuráveis
source_account_id = "851725566176"  # ID da conta de origem
dest_account_id = "851725566176"    # ID da conta de destino
source_bucket = "media-ingest-temporary"  # Bucket de origem
source_path = "1009000000/1009080000/"    # Caminho de origem
dest_bucket = "avs-vod-mc-input-2c87c40d939653bdbef99ff1ce204afc"  # Bucket de destino
dest_path = "1009000000/1009080000"  # Caminho de destino

# Intervalo de diretórios a copiar
start_dir = "1009080093"
end_dir = "1009080101"

# Chama a função para executar a cópia
copy_s3_files(source_account_id, dest_account_id, source_bucket, source_path, dest_bucket, dest_path, start_dir, end_dir)

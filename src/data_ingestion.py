#%%
import kagglehub
import os 
import shutil
from google.cloud import storage,bigquery
#%%

# Download latest version


def download_data(path_download,path_storage): 
    os.makedirs(path_storage,exist_ok=True) 
    for file in os.listdir(path_download):
        shutil.move(os.path.join(path_download,file),os.path.join(path_storage,file)) 

#%% 
#Send files for buckt in GCP 
def send_data_gcp(project,path_files,bucket_name):
    client = storage.Client(project=project)
    bucket = client.bucket(bucket_name)
    for file in os.listdir(path_files):
        try:
            print(f'----------Adicinando {file} ao bucket: {bucket}-------')
            blob = bucket.blob(f'{file.replace('_dataset','').replace('olist_','')}')   
            blob.upload_from_filename(os.path.join(path_files,file))
        except Exception as e: 
            print(f'---------Erro ao enivar o {file} ao bucket: {bucket}. Erro {e}')

#%%

def data_storage_to_bigquery(project, bucket, dataset):
    # Configurações de conexão do storage
    storage_client = storage.Client(project=project)
    bucket = storage_client.get_bucket(bucket)
    blobs = bucket.list_blobs()

    # Configurações de conexão do BigQuery
    bigquery_client = bigquery.Client(project=project)

    # Iniciando ingestão dos arquivos no BigQuery
    for file in blobs: 
        if file.name != 'order_reviews.csv':
            print(f'Iniciando ingestão do arquivo {file.name} no BigQuery...')

            # Formatar o caminho do arquivo no GCS
            file_path_storage = f'gs://{bucket.name}/{file.name}'

            # Criar a referência para o dataset
            dataset_ref = bigquery_client.dataset(dataset)

            table_ref = dataset_ref.table(f'raw_{file.name.replace(".csv", "")}')

            # Configuração de carregamento para o BigQuery
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV, 
                autodetect=True,  
                skip_leading_rows=1,  
                field_delimiter=',',  
                allow_jagged_rows=True 
            )

            load_job = bigquery_client.load_table_from_uri(
                file_path_storage,
                table_ref,
                job_config=job_config
            )

            load_job.result()

            print(f'Arquivo {file.name} adicionado com sucesso ao dataset {dataset} na tabela {table_ref}')



# %%

#Baixando os arquivos
path_download = kagglehub.dataset_download("olistbr/brazilian-ecommerce")
path_storage = '../data'
download_data(path_download,path_storage)

#Enviando os arquivos para o bucket no GCP
project = 'dbt-study-olist'
path_files = '../data'
bucket = 'olist_raw_data_dbt'
send_data_gcp(project,path_files,bucket)

#%%
#Enviando os arquivos para o bigquery no GCP
dataset = 'raw_olist'
data_storage_to_bigquery(project,bucket,dataset)


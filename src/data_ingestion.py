#%%
import kagglehub
import os 
import shutil
from google.cloud import storage
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

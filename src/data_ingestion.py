#%%
import kagglehub
import os 
import shutil
from google.cloud import storage
#%%

# Download latest version
path_download = kagglehub.dataset_download("olistbr/brazilian-ecommerce")
path_storage = '../data'

def download_data(path_download,path_storage): 
    os.makedirs(path_storage,exist_ok=True) 
    for file in os.listdir(path_download):
        shutil.move(os.path.join(path_download,file),os.path.join(path_storage,file)) 

#%% 
#Send files for buckt in GCP 

client = storage.Client()        

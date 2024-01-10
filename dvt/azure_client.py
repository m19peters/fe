from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
import azure.core.exceptions
import os

class AzureDataLake:
    def __init__(self):
        self.credential = DefaultAzureCredential()
        self.service = DataLakeServiceClient(account_url="https://fetrsaentreporting.dfs.core.windows.net/", credential=self.credential)

    def upload_safe(self,local_file_path:str, dest_dir: str):
        file_system_client = self.service.get_file_system_client(file_system="enterprise-reporting")
        directory_client = file_system_client.get_directory_client(os.path.dirname(dest_dir))
        
        file_client = directory_client.get_file_client(os.path.basename(local_file_path) + ".safe")

        with  open(local_file_path, mode='rb') as local_file:
            file_contents = local_file.read()

        file_client.upload_data(file_contents, overwrite=True)

        existing_directory_client = file_system_client.get_directory_client(os.path.dirname(dest_dir))
        existing_file_client = existing_directory_client.get_file_client(os.path.basename(local_file_path))
        if (existing_file_client.exists()):
            existing_file_client.delete_file

        file_client.rename_file(new_name=os.path.join("enterprise-reporting/",dest_dir))

    def upload(self,local_file_path:str, dest_dir: str):
        file_system_client = self.service.get_file_system_client(file_system="enterprise-reporting")
        directory_client = file_system_client.get_directory_client(os.path.dirname(dest_dir))
        
        file_client = directory_client.get_file_client(os.path.basename(local_file_path))
        
        with  open(local_file_path,'r') as local_file:
            file_contents = local_file.read()

        file_client.upload_data(file_contents, overwrite=True)

    def download(self, az_file_path:str, local_dl_path:str):
        file_system_client = self.service.get_file_system_client(file_system="enterprise-reporting")
        directory_client = file_system_client.get_directory_client(os.path.dirname(az_file_path))       
        
        with open(local_dl_path,'wb') as local_file:
            file_client = directory_client.get_file_client(os.path.basename(az_file_path))
            download = file_client.download_file()

            downloaded_bytes = download.readall()
            local_file.write(downloaded_bytes)
    
    def ls(self, az_file_path:str):
        file_system_client = self.service.get_file_system_client(file_system="enterprise-reporting")
        objects = []

        paths = file_system_client.get_paths(path=az_file_path)
        for path in paths:
            objects.append(path.name)
        
        return objects
    
    def exists(self, az_path:str):
        file_system_client = self.service.get_file_system_client(file_system="enterprise-reporting")
        objects = []
        exists = True

        try:
            paths = file_system_client.get_paths(path=az_path)
            for path in paths:
                objects.append(path.name)
                break
        except azure.core.exceptions.ResourceNotFoundError as e:
            exists = False

        return exists
    
if __name__ == "__main__":
    import azure_client

    az = azure_client.AzureDataLake()
    fs = az.ls("data_swamp/outgoing")
    for f in fs:
        print(f)

    exists = az.exists("data_swamp/outgoing/not_going_to_findme.csv")
    print(exists)

    exists = az.exists("data_lake/dstep=1_bronze/dstore=validator")
    print(exists)

    az.upload(f"C:\\Users\\60099\\code\\dvt\\validations\\sapeast_AUSP\\20230418_125055\\diff_summary_sapeast_AUSP.csv"
              , "data_lake/dstep=1_bronze/dstore=validator")
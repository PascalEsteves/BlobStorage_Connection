import json
import os
from datetime import datetime
from azure.storage.blob import BlobServiceClient,generate_blob_sas,BlobSasPermissions
import io
import pandas as pd
from dateutil.relativedelta import relativedelta


class Connection():

    def __init__(self, cfg: dict) -> None:

        self.user = cfg.blob_user
        self.key = cfg.blob_key
        self.client= BlobServiceClient(account_url=f"https://{self.user}.blob.core.windows.net",
                                       credential=self.key)
        self.not_uploaded = []

    def get_list_files_blob(self,
                            container_name:str,
                            folder=None)->list:

        client_container = self.client.get_container_client(container= container_name)

        if folder:
            files = [x.name for x in client_container.list_blobs(name_starts_with=folder)]
        else:
            files = [x.name for x in client_container.list_blobs()]

        return files

    def Get_blob_client(self, container_name, blob_name):
        return self.client.get_blob_client(container=container_name, blob= blob_name)

    def get_file_from_blob(self,
                           container_name:str,
                           blob_name:str)->object:

        file = self.client.get_blob_client(container=container_name, blob= blob_name)
        file_bytes = file.download_blob().readall()

        return file_bytes


    def get_sas_link(self,
                     container_name:str,
                     blob_name:str):

        file =  self.client.get_blob_client(container=container_name, blob=blob_name)

        token = generate_blob_sas(account_name=self.user,
                                      account_key = self.key,
                                      container_name = container_name,
                                      blob_name= blob_name,
                                      permission=BlobSasPermissions(read=True, tag=False),
                                      expiry=datetime.utcnow() + relativedelta(months=3)
                                      )

        return file.url + "?" + token


    def get_most_recent_file(self, container_name):

        blob_client = self.client.get_container_client(container=container_name)
        blobs = blob_client.list_blobs()

        most_recent_file = max(blobs, key=lambda blob: blob.last_modified)

        return most_recent_file.name

    def get_excel_file_from_blob(self, container_name: str) -> pd.DataFrame:

        blob_client = self.client.get_container_client(container=container_name)
        blobs = blob_client.list_blobs()
        aux=[]
        for index, blob_name in enumerate(blobs):

            blob_client = self.client.get_blob_client(container=container_name, blob=blob_name)
            excel_content = blob_client.download_blob().content_as_bytes()
            if index==0:
                with pd.ExcelFile(io.BytesIO(excel_content)) as xlsx:
                    df = pd.read_excel(xlsx)
            else:
                with pd.ExcelFile(io.BytesIO(excel_content)) as xlsx:
                    aux = pd.read_excel(xlsx)
                    df = pd.concat([df,aux])
        return df

    def get_json_from_blob(self, container_name: str, blob_name: str) -> pd.DataFrame:

        blob_client = self.client.get_blob_client(container=container_name, blob=blob_name)
        json_data = json.loads(blob_client.download_blob().content_as_text())

        return json_data

    def get_videos_blob(self, container: str) ->list:

        files = self.get_list_files_blob(container_name=container)
        videos = []
        for video in files:
            if video.lower().endswith('mp4') or video.lower().endswith('avi') or video.lower().endswith('.mov') or video.lower().endswith('wmv'):
                videos.append(video)
        return videos




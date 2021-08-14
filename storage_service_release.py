#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import json
from json import loads
import os
from os.path import (
    join,
    exists,
    abspath,
    dirname,
)


# In[2]:



file='config/storage_service_creds_release.json' # read credential and request information of storage API from this file. 
#pwd = dirname(abspath(__file__))
pwd= os.getcwd()
print(pwd)
path = join(pwd, file)
print(path)
if exists(path) is False:
    raise FileNotFoundError('File', path, 'not found')
f = open(path)
creds = loads(f.read()) 
#print(self.path)
get_token_url = creds['get_token_url']
create_file_url = creds['create_file_url']
get_upload_url = creds['get_upload_url']


# In[3]:


def get_token(url=get_token_url,
              headers ={'content-type': 'application/x-www-form-urlencoded'},
              client_id='salaryadmin'):
    body= {'grant_type': 'authenticate_site','client_id': client_id}
    r = requests.post(url, data=body, headers=headers)
    #print(r.content)
    response = r.json()
    return response['access_token']
    


# In[4]:


def create_file(lg,
                fileId,
                name,
                access_token,
                roles_to_read,
                frequency,
                date,        
                org_id,
                report_type,
                url=create_file_url,
                folderId = "00000000-0000-0000-0000-000000000000",             
                accessType="Protected",   
                logo ="logo",
                ):
    lg.print_log("roles_to_read")
    lg.print_log(roles_to_read)
    
    lg.print_log("report_type")
    lg.print_log(report_type)
    lg.print_log("CorrelationId: " +fileId + " FolderId: " + folderId +  " Name: "+ name + " Date: "+ date + " OrganizationID "+ org_id+ " url: " + url  )
    body = {
        "CorrelationId": fileId,
        "FileInfos": [
            {
                "FolderId": folderId,
                "FileId": fileId,
                "Name": name,
                "AccessType": "Protected",
                "RolesAllowedToAccess": roles_to_read,
                "MetaData": {
                    "Frequency": frequency,
                    "Date": date, 
                    "OrganizationID": org_id ,
                    "ReportType": report_type
                }
            }
        ]
    }
    headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + access_token}

    r = requests.post(url, data=json.dumps(body), headers=headers)
    lg.print_log(r.status_code)
    return r.status_code


# In[5]:


def get_upload_url(lg,
                   fileId,
                   access_token,
                   url = get_upload_url):
    body = {
        "FileIds": [
            fileId
        ]
    }
    headers = {'Content-Type': 'application/json',
               'Authorization': 'Bearer ' + access_token,
               'Accept-Encoding' :'gzip, deflate, br',
               'Connection' : 'keep-alive',
               'x-service-id':'restaurants'
              }
    r = requests.post(url, data=json.dumps(body), headers=headers)
    lg.print_log(r.content)
    response = r.json()
    result = response['result']
    uploadUrl=""
    for i in result:
        uploadUrl= i["uploadUriWithSas"]
    return uploadUrl
    


# In[6]:


def file_upload(uploadUrl,path,lg):
    headers = {'Content-type': 'text/csv','x-ms-blob-type' : 'BlockBlob'}
    r = requests.put(uploadUrl, data=open(path, 'rb'), headers=headers)
    lg.print_log(r.content)
    lg.print_log(r.status_code)
    return r.status_code


# In[ ]:





#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
from os.path import (
    join,
    exists,
    abspath,
    dirname,
)
import uuid
from storage_service_release import(
    get_token,
    create_file,
    get_upload_url,
    file_upload,
)
from datetime import datetime, timedelta


# In[2]:


def func_storage_service_driver(fileName,roles_allowed_to_read,frequency,date,org_id,report_type,lg):
    #get_token
    access_token = get_token()
    lg.print_log(access_token)
    fileId= uuid.uuid4()
    
    try:
        #create_file
        status = create_file(
            fileId= str(fileId),
            name = fileName,
            access_token= access_token,
            roles_to_read=roles_allowed_to_read,
            frequency = frequency,
            date=  date,      
            org_id= org_id,
            report_type= report_type,
            lg=lg

        )
        file_creation_status =0
        retry_count =0
        while (file_creation_status == 0 and retry_count <=60) : #retry file creating 60 times
            if status==202:
                file_creation_status =1
                lg.print_log("File create request accepted : " + str(fileId)+" "+ fileName + " " + access_token)         
                #get_upload_url
                count =0
                #UploadUrl= ""
                UploadUrl = get_upload_url(access_token=access_token,fileId= str(fileId),lg=lg)
                lg.print_log(UploadUrl)
                while((not UploadUrl) and count <=60 ): # retry get url 60 times
                    UploadUrl = get_upload_url(access_token=access_token,fileId= str(fileId),lg=lg)
                    lg.print_log(UploadUrl)
                    count= count+1
                if count ==60 and (not uploadUrl):
                    lg.print_log('File getting url failed after retrying for 1 minute')
                    lg.print_log("Getting url failed : "+ str(fileId)+" "+ fileName + " " + access_token)
                else:  
                    lg.print_log("File created : UploadUrl " + UploadUrl + " "+ str(fileId)+" "+ fileName + " " + access_token)
                    file = fileName
                    pwd= os.getcwd()
                    path = join(pwd, file)
                    lg.print_log(path)

                    #file_upload

                    upload_status = file_upload(
                        path=path,
                        uploadUrl=UploadUrl,
                        lg=lg
                    )
                    #lg.print_log(upload_status)
                    if upload_status == 201:
                        lg.print_log(upload_status)
                        lg.print_log("File Uploaded : " +fileName + " Path " +path + " UploadUrl "+ UploadUrl)
                    else:
                        lg.print_log("File Upload Failed :Path "+path + "UploadUrl "+ UploadUrl)
                        upload_retry_count = 0
                        while(upload_status != 201 and upload_retry_count <= 60): #retry file uploading 60 times
                            lg.print_log("File Upload Retrying :Path "+path + "UploadUrl "+ UploadUrl)
                            upload_status = file_upload(
                                path=path,
                                uploadUrl=UploadUrl,
                                lg=lg
                            )
                            if upload_status == 201:
                                lg.print_log(upload_status)
                                lg.print_log("File Uploaded : " +fileName + " Path " +path + " UploadUrl "+ UploadUrl)
                            upload_retry_count = upload_retry_count+1
                        if upload_status != 201:
                            lg.print_log("File Upload Failed Finally :Path "+path + "UploadUrl "+ UploadUrl)

            else:
                lg.print_log("File creation failed : "+ str(fileId)+" "+ fileName + " " + access_token )
                lg.print_log("File creation retrying")
                retry_count = retry_count+1

                # retry create_file
                status = create_file(
                    fileId= str(fileId),
                    name = fileName,
                    access_token= access_token,
                    roles_to_read=roles_allowed_to_read,
                    frequency = frequency,
                    date=  date,      
                    org_id= org_id,
                    report_type= report_type,
                    lg=lg

                )
    except Exception as e:
        lg.print_log(e)
            
    


# In[ ]:





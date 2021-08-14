#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from datetime import datetime, tzinfo,timezone
import calendar
from dateutil import tz
from pandas.io.json import json_normalize  
from pandas import (
     to_numeric,
     to_datetime
)
from bson.tz_util import FixedOffset
import json
import numpy as np
import pymysql
import os
import datetime
from pymongo import MongoClient

from os.path import join
from datetime import datetime, timedelta

from os.path import (
    join,
    basename,
    abspath,
    splitext,
    dirname,
)
import uuid
from mongo_manager import MongoDBManager
from storage_service_driver_release import func_storage_service_driver
from log_store_service import Logger



# In[2]:


def final_export_column_rename(dataframe):
    """
    Rename final excel file attibute names as per requirements
    
    """
    dataframe = dataframe.rename(
        columns= {
            'CreatedDate': 'Created Date',
            'RequestSubmissionDateTime' :'Request Date & Time',
            'OrganizationName' : 'Organization Name',
            'EmployeeName' : 'Employee Name',
            'EmployeeDisplayId' :'Employee ID',
            'DipositDateAndTime' :'Transaction Date & Time',
            'EmployeeBankAccountNumber' :'Account No',
            'DepositAmount' :'Disbursed Amount',
            'ConvenienceFee' : 'Convenience Fee',
            'BankTransactionId' : 'Bank Ref No',
            'RequestDisplayId' : 'Request ID',
            'FinPartnersCommission' : 'IPDC Convenience Fee' ,
            'OperatorOrgsCommission' : 'Shohoz Convenience Fee' ,
        }
    )
    return dataframe


# In[3]:


#Transaction History - Shohoz superadmin(admin), Shohoz admin(earnedwageadmin), org admin(orgadmin_{{orgId}}), shohoz accounts(earnedwageaccount), IPDC admin(finpartner_{{finOrgId}})
def roles_allowed_to_read(orgId,finOrgId):
    roles = ["sysadmin","earnedwageadmin","orgadmin_"+orgId,"earnedwageaccount","finpartneruser_"+finOrgId,"finpartner_"+finOrgId]
    return roles


# In[4]:


def data_extract_organizations(lg):
    """
    Extract all organizations' data from SampleDatebase and Sample collection.

    Raises:
    Exception: when the connector fails
    to establish connecting to database
    
    """
    try:
        client = MongoDBManager()
        collection = client.get_collection(dbname = 'SampleDatabase',collection_name ='SampleCollectionName')
        projection = {}
        query = {}
        projection["_id"] = 1
        projection["OrganizationName"] = 1
        projection["FinancialPartnerOrgConfig.FinancialPartnerOrgId"] = 1
        cursor = collection.find(query, projection = projection)
        df = pd.DataFrame(list(cursor))
        lg.print_log ("Data extraction of organizations complete")
        df['FinancialPartnerOrgConfig'] =  df['FinancialPartnerOrgConfig'].apply(lambda x: x['FinancialPartnerOrgId'])
        df =df.rename(
            columns ={
                '_id' : 'OrganizationId'
            }
        )

    except Exception as e:
        lg.print_log(e)
    return df


# In[5]:


def data_extract_deposit_history(lg,number_days= 1):
    
    """
    Extract last days deposit history data from SampleDatebase2 and Sample2 collection.

    Raises:
    Exception: when the connector fails
    to establish connecting to database
    
    """
    
    try:
        client = MongoDBManager()
        collection = client.get_collection(dbname = 'SampleDatebase2',collection_name ='SampleCollection2')
        query = {}
        date = (datetime.now() - timedelta(number_days)).replace(hour=0,minute=0,second=0,microsecond=0)
        date_end = date + timedelta(1)
        query = {}
        query["CreatedDate"] = {
             u"$gte": date,
             u"$lte" : date_end

         }
        print(date)
        print(date_end)
        projection = {}
        projection["RequestDisplayId"] = 1
        #projection["OrganizationName"] = 1
        projection["OrganizationId"] = 1
        projection["EmployeeName"] = 1
        projection["EmployeeDisplayId"] = 1
        projection["EmployeeUserId"]=1
        projection["DipositDateAndTime"] = 1
        projection["EmployeeBankAccountNumber"] = 1
        projection["DepositAmount"] = 1
        projection["BankTransactionId"] = 1
        projection["RequestSubmissionDateTime"] = 1
        projection["ConvenienceFee"] = 1
        projection["CreatedDate"] = 1
        cursor = collection.find(query, projection = projection)
        df = pd.DataFrame(list(cursor))
        lg.print_log ("Data extraction of deposit_history complete")
        df =df.rename(
        columns= {
            "EmployeeUserId" : "user_id"
            }
        )
    except Exception as e:
        lg.print_log(e)

    return df


# In[6]:


def data_extract_employee_records(lg):

    """
    Extract all employee data from SampleDatabase3 and SampleCollection3.

    Raises:
    Exception: when the connector fails
    to establish connecting to database
    
    """
    try:
        client = MongoDBManager()
        collection = client.get_collection(dbname = 'SampleDatabase3',collection_name ='SampleCollection3')
        projection = {}
        query ={}
        projection["Designation"] = u"$Designation"
        cursor = collection.find(query, projection = projection)
        df = pd.DataFrame(list(cursor))
        lg.print_log ("Data extraction of employee_records complete")
        df = df.rename(
            columns= {
                '_id' : 'user_id'
            }

        )      
    except Exception as e:
        lg.print_log(e)
    return df


# In[7]:


def data_extract_commission(lg,number_days): 
    """
    Extract convenience fee data from SampleDatabase4 and SampleCollection4.

    Raises:
    Exception: when the connector fails
    to establish connecting to database
    
    """
    try:
        client = MongoDBManager()
        collection = client.get_collection(dbname = 'SampleDatabase4',collection_name ='SampleCollection4')
        projection = {}
        query = {}
        date = (datetime.now() - timedelta(number_days)).replace(hour=0,minute=0,second=0,microsecond=0)
        date_end = date + timedelta(1)
        query = {}
        query["CreatedDate"] = {
             u"$gte": date,
             u"$lte" : date_end

         }
        print(date)
        print(date_end)
        projection["FinPartnersCommission"] = 1
        projection["OperatorOrgsCommission"] = 1
        projection["OrganizationId"] = 1
        projection["RequestDisplayId"] =1
        cursor = collection.find(query, projection = projection)
        df = pd.DataFrame(list(cursor))
        lg.print_log ("Data extraction of RequestStatusViewModels complete")
        print(df.head())
#         df = df.rename(
#             columns= {
#                 '_id' : 'user_id'
#             }

#         )      
    except Exception as e:
        lg.print_log(e)
    return df
    


# In[8]:


def df_to_csv(date,df,org_name):
    """
    This method generate the filename and converts the dateframe to csv
    
    """
    fileName= date+'_Transaction_Report_'+org_name+'.csv'
    df.to_csv(fileName,index= False)
    


# In[9]:


def extract_org_details(df,lg):
    """
    This method used to extract OrganizationName, OrganizationId and FinId from  dataframe
    
    """
    org_name_list=df['OrganizationName'].unique()
    lg.print_log(org_name_list)
    org_name = str(org_name_list[0])
    lg.print_log(org_name)
    org_id_list = df['OrganizationId'].unique()
    org_id = str(org_id_list[0])
    lg.print_log(org_id)
    finId_list = df['FinancialPartnerOrgConfig'].unique()
    finId= str(finId_list[0])
    lg.print_log(org_name)
    return org_name,org_id,finId


# In[10]:


def final_export_merge_and_store(dfs,date,lg,no_transaction_record,df_to_export):
    """
     This method is used to generating and storing the final dataframe which will be converted to csv. 
     Storage service API is used to store the generated file.
     
    """
    lg.print_log("total organization : " + str(len(dfs)))
    print(df_to_export.shape)
    for i in range(0,len(dfs)):
        org_name,org_id,finId = extract_org_details(dfs[i],lg=lg)           
        fileName= date+'_Transaction_Report_'+org_name+'.csv' 
        
        if no_transaction_record:
            dfs[i]= dfs[i].drop(
                columns = [
                    "OrganizationId",
                    "FinancialPartnerOrgConfig"
                    ]
                )
            df_to_csv(date,dfs[i],org_name)
            
        else:
            
            export_df = dfs[i].merge(
                df_to_export,
                left_on = 'OrganizationId',
                right_on ='OrganizationId',
                how='left'
            )
            export_df= export_df.drop(
                columns = [
                    "OrganizationId",
                    "user_id",
                    "FinancialPartnerOrgConfig",
                    "_id"
                    ]
                ) 
            export_df = final_export_column_rename(export_df)  
            df_to_csv(date,export_df,org_name)
            
        roles_allowed = roles_allowed_to_read(org_id,finId)
        lg.print_log(roles_allowed)
        
        func_storage_service_driver(
            fileName= fileName,
            roles_allowed_to_read= roles_allowed,
            frequency= 1, #Frequency enum { 1 - Daily 2 - Monthly 3 - Yearly }
            date= date,
            org_id= org_id,
            report_type = 1, #ReportType enum { 1 - Transaction History 2 - Transfer Log 3 - Payable To Shohoz }
            lg=lg
        )  


# In[11]:


if __name__ == "__main__":
    
    lg = Logger("Transaction_Report_Release.py")
    lg.print_log("Earned wage script started...")
    
    try:  
        number_days= 1  #To generate the report of the previous day
        df_org = data_extract_organizations(lg=lg)
        df_deposit = data_extract_deposit_history(lg=lg,number_days=number_days)
        df_employee = data_extract_employee_records(lg=lg) 
        df_commission = data_extract_commission(lg=lg,number_days=number_days)
        no_transaction_record = False
        df_to_export = pd.DataFrame()
        final_export = pd.DataFrame()
        if df_deposit.empty:
            no_transaction_record = True
            
        else:
            dff= df_deposit.merge(
            df_employee,
            left_on= "user_id",
            right_on= "user_id",
            how='left'
            )
            lg.print_log(dff.shape)
            
            
            df_to_export = dff[[
                           'RequestSubmissionDateTime',
                           'RequestDisplayId',
                           'OrganizationId',
                           'EmployeeDisplayId',
                           'EmployeeName',
                           'Designation',
                           'EmployeeBankAccountNumber',
                           'BankTransactionId',
                           'DipositDateAndTime',
                           'DepositAmount',
                           'ConvenienceFee',
                           'user_id'
                          ]]
            df_to_export['DipositDateAndTime'] = df_to_export['DipositDateAndTime'].apply(
                lambda x: datetime.fromtimestamp(calendar.timegm(x.timetuple()))
            )
            df_to_export['RequestSubmissionDateTime'] = df_to_export['RequestSubmissionDateTime'].apply(
                lambda x: datetime.fromtimestamp(calendar.timegm(x.timetuple()))
            )
            
            df_to_export['DipositDateAndTime'] = df_to_export['DipositDateAndTime'].apply(lambda x: x.strftime('%d/%m/%Y %I:%M:%S'))
            df_to_export['RequestSubmissionDateTime'] = df_to_export['RequestSubmissionDateTime'].apply(lambda x: x.strftime('%d/%m/%Y %I:%M:%S'))
            
            
            final_export = df_to_export.merge(
                df_commission,
                left_on = [
                    'OrganizationId',
                    'RequestDisplayId'
                ],
                right_on = [
                    'OrganizationId',
                    'RequestDisplayId'
                ]
            )
            
        dfs = [x for _, x in df_org.groupby('OrganizationName')]
        date=datetime.strftime(datetime.now() - timedelta(number_days), '%d-%m-%Y')
        date= date.replace('T', ' ')
        final_export_merge_and_store(dfs=dfs,date=date,lg=lg,no_transaction_record=no_transaction_record,df_to_export=final_export)
        lg.logger_close()     
        
    except Exception as e:
        lg.print_log(e)
        lg.logger_close()
        


# In[ ]:





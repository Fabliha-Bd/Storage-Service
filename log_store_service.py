#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from database_manager import DatabaseManager
from helper import (
    add_quote,
    write_to_db,
)
import os
from os.path import (
    join,
    exists,
    abspath,
    dirname,
)

class Logger:
    __pwd = os.getcwd()
    script_name = ""
    table_name= "tabel_name"
    __creds_file=""
        
    def __init__(self,script_name):
        self.script_name =script_name
        self.__creds_file = join(
            self.__pwd,
            'db_credential_file.json',
        )
        self.__db_write = DatabaseManager(self.__creds_file)
        
    def print_log(self,message):
        try:
            
            if isinstance(message, pd.DataFrame):
                message= message.to_string()
                print(message)

            data = {
                'log' : [message],
                'script_name' : [self.script_name]
            }
            dff = pd.DataFrame(data)
            #print(dff)
            write_to_db(
                db=self.__db_write,
                table=self.table_name,
                df=dff,
            )
        except Exception as e:
            print(e)
            data = {
                'log' : "script working, but log text invalid",
                'script_name' : [self.script_name]
            }
            dff = pd.DataFrame(data)
            #print(dff)
            write_to_db(
                db=self.__db_write,
                table=self.table_name,
                df=dff,
            )
            
    def logger_close(self):
        self.__db_write.close()


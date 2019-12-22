import psycopg2
import csv
from datetime import datetime
import os

def writeErrorLogFile(errors):
    now=datetime.today()
    datetime_now = now.strftime("%d/%m/%Y %H:%M:%S")
    opendFile= open("ErrorsLogRecordsPostgreSql.txt","a",encoding="utf")
    opendFile.write(errors+" "+datetime_now+"\n")
    
        

def writeErrorLogFileMongo(errors):
    pc_name=os.environ['COMPUTERNAME']
    errors +=pc_name
    now=datetime.today()
    datetime_now = now.strftime("%d/%m/%Y %H:%M:%S")
    opendFile=open("ErrorsLogRecordsMongoDB.txt","a",encoding="utf") 
    opendFile.write(errors+" "+datetime_now+"\n")
   
        
    
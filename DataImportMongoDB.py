import pandas as pd 
import pymongo
import json
import csv
import os
import MongoDB
import PerformanceOfDatabase
import writeToErrorsInLogFiles



class DataImportMongoDB:

    rowCount=0
    def __init__(self,filepath):
        self.filepath = filepath

    def CsvtoMongoDB(self,host,database,collection,rowCount,chunkSize,domain_index,domains):
        self.domain_index=domain_index
        self.domains=domains
        self.rowCount=rowCount
        self.chunkSize=chunkSize
        self.host=host
        self.database=database
        self.collection=collection
        self.mongoDb=MongoDB.MongoDB(self.host,self.database,self.collection)
        durum=0
        
        try:
            if len(domain_index) !=0:
                for chunk in pd.read_csv(self.filepath,skiprows=1+self.rowCount, chunksize=self.chunkSize,usecols=self.domain_index,names=self.domains,low_memory=False,encoding='utf'): 
                    try:
                        data_json = json.loads(chunk.to_json(orient='records'))
                        durum=1
                    except:
                        print("Csv to json coverting error")
                        error="Csv to json coverting error ---> user name "
                        writeToErrorsInLogFiles.writeErrorLogFileMongo(error)
                        durum=0               
                    break
            elif len(domain_index)==0:
                for chunk in pd.read_csv(self.filepath,skiprows=1+self.rowCount, chunksize=self.chunkSize,names=self.domains,low_memory=False,encoding='utf'): 
                    try:
                        data_json = json.loads(chunk.to_json(orient='records'))
                        durum=1
                    except:
                        print("Csv to json coverting error")
                        error="Csv to json coverting error ---> user name "
                        writeToErrorsInLogFiles.writeErrorLogFileMongo(error)
                        durum=0               
                    break
        except:
            print("Csv reading error")
            error="Csv reading error ---> user name "
            writeToErrorsInLogFiles.writeErrorLogFileMongo(error)
            durum=0              
        if durum==1:
            self.mongoDb.InsertMany(data_json)
            durum=1
        else:
            pass
        self.mongoDb.ConnectionClose()
        
        


import pymongo
import writeToErrorsInLogFiles

class MongoDB:

    def __init__(self,host,database,collection):
        
        try:
            self.myclient = pymongo.MongoClient(host)
            self.mydb = self.myclient[database]
            self.mycol = self.mydb[collection]
            self.connect= True
        except:
            print("MongoDB connection error")
            error="MongoDB connection error ---> user name "
            writeToErrorsInLogFiles.writeErrorLogFileMongo(error)
            self.connect= False

    def ConnectionClose(self):
        self.myclient.close()
        
    def InsertMany(self,dataJson):
        try:
            self.mycol.insert_many(dataJson, ordered=False, bypass_document_validation=True)
        except:
            print("MongoDB Insermany error")
            error="MongoDB Insermany error ---> user name "
            writeToErrorsInLogFiles.writeErrorLogFileMongo(error)

        
    
    def InsertOne(self,dataJson):
        try:
            self.mycol.insert_one(dataJson)
        except:
            print("MongoDB InserOne error")
            error="MongoDB InserOne error ---> user name "
            writeToErrorsInLogFiles.writeErrorLogFileMongo(error)
    
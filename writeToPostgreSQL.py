import psycopg2
import csv
import pandas
import pandas as pd
import writeToErrorsInLogFiles
from io import StringIO
from collections import namedtuple
from psycopg2 import sql, connect


class WPsql():
    #connecting postgresql
    def connDB(self,user1,password1,host1,port1,dbName1):
        try:
            global connection 
            connection = psycopg2.connect(user=user1,
                                    password=password1,
                                    host=host1,
                                    port=port1,
                                    database=dbName1)
            
            global cursor 
            cursor = connection.cursor()

            #print("POSTGRESQL CONNECTION IS OPENED\n")
            return True

        except (Exception, psycopg2.Error) as error:
            err="Error while connecting to PostgreSQL "+str(error)
            writeToErrorsInLogFiles.writeErrorLogFile(err)
            print (err)
            return False

    #disconneting postgresql
    def disconnDB(self):
            if(connection):
                cursor.close()
                connection.close()
                #print("POSTGRESQL CONNECTION IS CLOSED")

    #insert table with requested columns in CSV filesto postgresql
    def InsertRequestedColumns(self,dbName,columns,rows,tableName):
        cols=self.spaceControl(columns)
        cols=", ".join(cols)
        query_placeholders = ', '.join(['%s'] * len(columns))
        query_columns = self.spaceControl(columns)
        query_columns = ', '.join(query_columns)
        for line in rows:
            insert_query='''INSERT INTO %s (%s) VALUES (%s)''' %(tableName,query_columns,query_placeholders)
            cursor.execute(insert_query,line)
        
        connection.commit()
    #check table name in given database
    def tableControl(self,dbName,tableName):
        cursor.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
        tables=cursor.fetchall()
        counter=0
        for i in tables:
            i="".join(str(i).split(","))
            i="".join(str(i).split("'"))
            i="".join(str(i).split("("))
            i="".join(str(i).split(")"))
            tables[counter]=i
            counter+=1
        if (tableName in tables):
            return True
        return False

    #get column names in given table 
    def getColumnsNames(self,dbName,tableName):
        columns=[]
        col_names_str = "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE "
        col_names_str += "table_name = '{}';".format( tableName )
        try:
            sql_object = sql.SQL(col_names_str).format(sql.Identifier( tableName ))

            cursor.execute( sql_object )

            col_names = ( cursor.fetchall() )

            for tup in col_names:
                columns += [ tup[0] ]
            
        except Exception as err:
            print ("get_columns_names ERROR:", err)
        return columns
        
    #seperate all colums from other variables(for requested columns)
    def seperatingColumns(self,dbName,indeks,fileName,tableName,headerInfo):
        try:
            headers = None
            results = []
            newCols=[]
            isEqCols=[]
            cols=self.getColumnsNames(dbName,tableName)
            
            reader = csv.reader(open(fileName),encoding='utf')
            for row in reader:
                if not headers:
                    headers = []
                    for i in indeks.split():
                        headers.append(int(i))
                        ###########################
                    for c in ([row[i] for i in headers]):
                        if (c in cols):
                            pass
                        else:
                            print("The requested columns in the table do not exist.")
                            return False
                            ###################################
                    if(headerInfo=="1"):
                        results.append(list([row[i] for i in headers]))
                else:
                    results.append(list([row[i] for i in headers]))

            self.InsertRequestedColumns(dbName,cols,results,tableName)
        
        except FileNotFoundError as error:
            err="Error while founding file "+str(error)
            writeToErrorsInLogFiles.writeErrorLogFile(err)
            print("File Not Found")

    #Check the spacing between given words
    def spaceControl(self,data):
        i=0
        newList=[]
        for d in data:
            veri="_".join(d.split())
            veri="_".join(veri.split("'"))
            veri="_".join(veri.split("/"))
            newList.append(veri)
            data[i]=veri
            i+=1       
        return data


    #copy all columns in csv to postgresql 
    def copyAll(self,user,password,host,port,dbName,fileName,tableName,headerInfo,sep1,init_size,chunk_size):
        try:
            bln=self.connDB(user,password,host,port,dbName)
            sum=0
            lines=self.getColumnsNames(dbName,tableName)
            query_placeholders = ', '.join(['%s'] * len(lines))
            query_columns = self.spaceControl(lines)
            query_columns = ', '.join(query_columns)
            insert_query='''INSERT INTO %s (%s) VALUES (%s)''' %(tableName,query_columns,query_placeholders)
            
            if(headerInfo=="0"):#there are headers in csv files
                with open(fileName) as file:
                    for line in file:
                        if 0==0 :
                            pass
                        file.close()
                        break

                for chunk in pd.read_csv(fileName,sep=sep1,skiprows=init_size, chunksize=chunk_size,low_memory=False,encoding='utf'):
                    for lines in chunk.values:
                        cursor.execute(insert_query,lines)
                    break
                connection.commit()
            elif(headerInfo=="1"):#there is not header in csv files
                if(init_size==0):
                    cursor.execute(insert_query,lines)
                for chunk in pd.read_csv(fileName,sep=sep1,skiprows=init_size, chunksize=chunk_size,low_memory=False,encoding='utf'):
                    for lines in chunk.values:
                        cursor.execute(insert_query,lines)
                    break
                connection.commit()    
            else:
                print("there is not header information")
            bln=self.disconnDB()
        except FileNotFoundError as error:
            err="Error while founding file "+str(error)
            writeToErrorsInLogFiles.writeErrorLogFile(err)
            print("File Not Found")

    #calculate row count in csv
    def rowCounter(self,fileName,headerInfo):
        try:
            with open(fileName, 'r', encoding="utf") as readFile:
                rowCount=sum(1 for line in readFile)
            if(headerInfo=="1"):
                return rowCount
            else:
                return rowCount-1
        except FileNotFoundError as error:
            err="Error while founding file "+str(error)
            writeToErrorsInLogFiles.writeErrorLogFile(err)
            print("File Not Found")

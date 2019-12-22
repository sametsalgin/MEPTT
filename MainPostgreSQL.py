import writeToPostgreSQL
import writeToXMLfilesForPsql
import writeToErrorsInLogFiles
import os
import sys
import datetime
from timeit import default_timer
import sendToMailPsql
import multiprocessing
import psutil
def main():

    print("*************************************")
    user=""
    password=""
    host=""
    port=""
    dbName=""
    nesne1=writeToPostgreSQL.WPsql()
    while True:
        user=input("user                                --")
        password=input("password                            --")
        host=input("host                                --")
        port=input("port                                --")
        dbName=input("Database Name                       --")
        psqlIsConn=nesne1.connDB(user,password,host,port,dbName)
        if(psqlIsConn==True):
            print("CONNECTED\n")
            break
        else:
            print("Database not founded !!! \n")
            os.system("pause")
            os.system("cls")
            print("*************************************")
    while True:
        fileName=input("File Name                           --")
        if(os.path.exists(fileName)):
            break
        else:
            print("File not exists. Please enter again file name!!!\n")
            writeToErrorsInLogFiles.writeErrorLogFile(fileName+" -> file not exists")
    while True:
        tableName=input("Table Name                          --")
        tableName=tableName.lower()
        if(nesne1.tableControl(dbName,tableName)==True):
           break
        else:
           print("Table not exists. Please create table!!!\n")
           writeToErrorsInLogFiles.writeErrorLogFile(tableName+" -> table not exists in database = {}".format(dbName))
    while True:
        headerInfo=input("Header(0)/ No Header(1)             --")
        if(headerInfo=="0" or headerInfo=="1"):
            break
        else:
            print("Header(0)/ No Header(1) wrong choice!Please enter again!")
    while True:
        columnState=input("Required Columns(0)/ All Columns(1) --")
        if(columnState=="0"):
            desiredColumnIndexes=input("Index of Required Columns           --")
            break
        elif(columnState=="1"):
            desiredColumnIndexes=""
            break
        else:
            print("Required Columns(0)/ All Columns(1) is wrong choice!Please enter again!")

    sep=','
 
    toMail=input("Enter the Gmail address             --")
    print("*************************************")
    sendToMailPsql.sendMailStart(toMail)
    rowCount=nesne1.rowCounter(fileName,headerInfo)
    print("Row Count: {}".format(rowCount))
    core_count=multiprocessing.cpu_count()
    ram_available=(psutil.virtual_memory().available/(1024*1024*1024))
    calculate_chunk_size=int(ram_available/(core_count*2)*1000000)
    if (rowCount/core_count)<calculate_chunk_size:
        chunk_size=int(rowCount/core_count)
        chunk_size=max(chunk_size,1)
    else:
        chunk_size=calculate_chunk_size
        chunk_size=max(chunk_size,1)
    init_size=0
    count=multiprocessing.cpu_count()
    size=0
    procs=[]
    if(columnState=="0" ):
        start_time = default_timer()
        nesne1.seperatingColumns(dbName,desiredColumnIndexes,fileName,tableName,headerInfo)
    else:
        nesne1.disconnDB()
        start_time = default_timer()
        while True:
            try:
                if size<count:
                    if init_size+1>rowCount:
                        break                                                     
                    proc=multiprocessing.Process(target =nesne1.copyAll, args = (user,password,host,port,dbName,fileName,tableName,headerInfo,sep,init_size,chunk_size))
                    procs.append(proc)
                    proc.start()
                    init_size +=chunk_size
                    size +=1
                    for one_proc in procs:
                        if one_proc.is_alive()== False:
                            procs.remove(one_proc)
                            size -=1
                if init_size+1>rowCount:
                    break
            except:
                break
            for one_proc in procs:
                if one_proc.is_alive()== False:
                    procs.remove(one_proc)
                    size -=1

    while True:
        if len(procs)==0:
                break
        for one_proc in procs:
            if one_proc.is_alive()== False:
                procs.remove(one_proc)
                size -=1
            if len(procs)==0:
                break
    duration = default_timer() - start_time
    print("Lines per second: ",rowCount/duration)

    writeToXMLfilesForPsql.writeXML(dbName,tableName,datetime.datetime.now(),user,password,host,port) 
    nesne1.disconnDB()
    
    
    
    sendToMailPsql.sendMailFinish(toMail,str(duration),rowCount,rowCount/duration)
    print("DURATION: ",duration)
    print("PostgreSQL Database Operation Finished\n")

import MongoDB
import DataImportMongoDB
import PerformanceOfDatabase
import multiprocessing
import csv
import psutil
import os
import writeToErrorsInLogFiles
import sendToMailPsql

def size_calculate(fileName,line_num,domains_of_csv):
    sum=0
    with open(fileName, 'r', encoding="utf") as readFile:
        for line in readFile:
            if sum==0:
                domains=line.split(",")
                domains[len(domains)-1]=domains[len(domains)-1].replace("\n","")
            sum +=1
        line_num[sum]=sum
        domains_of_csv[0]=domains

def main():
    filepath=""
    host=""
    database=""
    collection=""
    core_count=multiprocessing.cpu_count()
    ram_available=(psutil.virtual_memory().available/(1024*1024*1024))
    calculate_chunk_size=int(ram_available/(core_count*(15/10))*1000000)

    performance=PerformanceOfDatabase.PerformanceOfDatabase()
    print("*************************************")
    while True:
        filepath=input("file path                           --")
        if(os.path.exists(filepath)):
            break
        else:
            print("File not exists. Please enter again file name!!!\n")

    nesne=DataImportMongoDB.DataImportMongoDB(filepath)
    
    print("*************************************")

    #size calculate
    manager = multiprocessing.Manager()
    size_of_csv=manager.dict()
    domains_of_csv=manager.dict()
    proc=multiprocessing.Process(target = size_calculate, args = (filepath,size_of_csv,(domains_of_csv)))
    proc.start()
    



    #Mongo connect condition
    while True:
        host=input("host                                --")
        database=input("database                            --")
        collection=input("collection                          --")
        mongo_connect=MongoDB.MongoDB(host,database,collection)
        if mongo_connect.connect==True:
            break
        else:
            print("Database not founded !!! \n")
            os.system("pause")
            os.system("cls")
            print("*************************************")

    print("*************************************")
    toMail=input("Enter the Gmail address             --")

    sendToMailPsql.sendMailStartMongo(toMail)
    
    print("*************************************")
    #header info 
    while True:
        header_info=input("Header(0)/ No Header(1)             --")
        if header_info =="1" or header_info =="0":
            if header_info=="0":
                break
            else:
                print("if no header csv is not convert json")
            
        else:
            print("please correct value !!! \n")
            os.system("pause")
            os.system("cls")
            print("*************************************")

    file_size=os.stat(filepath).st_size/(1024*1024)       
    proc.join()
    for alfa in size_of_csv.values():
        limit=int(alfa)
    domains=domains_of_csv.values()
    domains=domains[0]
    limit -=1
    selected_columns=[]
    selected_columns_index=[]

    print("*************************************")
    while True:
        column_state=input("Required Columns(0)/ All Columns(1) --")
        if column_state =="1":
            print("selected all columns")
            break
        elif column_state =="0":
            print(domains)
            while True:
                column_count=input("column count:                       --")
                try:
                    column_count=int(column_count)
                    if column_count>len(domains):
                        print("columns count is not bigger than all columns count")
                    else:
                        break
                except:
                    print("please enter integer value \n" )
            try:
                column_count=int(column_count)
                counter=0
                while counter<column_count:
                    enter_column_name=input("enter column name            --")
                    if enter_column_name not in domains:
                        print("please right column name \n")
                    else:
                        find_index=domains.index(enter_column_name)
                        selected_columns.append(enter_column_name)
                        selected_columns_index.append(find_index)
                        counter +=1
                domains=selected_columns
                break
            except:
                print("SYSTEM ERROR")
        else:
            print("please correct value !!! \n")
            os.system("pause")
            os.system("cls")
            print("*************************************")

    print("*************************************")   
    if (limit/core_count)<calculate_chunk_size:
        chunk_size=int(limit/core_count)
        chunk_size=max(chunk_size,1)
    else:
        chunk_size=calculate_chunk_size
        chunk_size=max(chunk_size,1)
    init_size=0
    procs=[] #procesing list
    working_proc_count=0
    performance.TimePerformanceStart()
    while True:
        try:
            if working_proc_count<core_count:
                if init_size+1>limit:
                    break
                proc=multiprocessing.Process(target = nesne.CsvtoMongoDB, args = (host,database,collection,init_size,chunk_size,selected_columns_index,domains,))
                procs.append(proc)
                proc.start()
                init_size +=chunk_size
                working_proc_count +=1
                for one_proc in procs:
                    if one_proc.is_alive()== False:
                        procs.remove(one_proc)
                        working_proc_count -=1
            if init_size+1>limit:
                break
        except:
            break
        for one_proc in procs:
            if one_proc.is_alive()== False:
                procs.remove(one_proc)
                working_proc_count -=1
    

    while True:
        if len(procs)==0:
                break
        for one_proc in procs:
            if one_proc.is_alive()== False:
                procs.remove(one_proc)
                working_proc_count -=1
            if len(procs)==0:
                break
        
    pipeline,time_difference=performance.TimePerformanceEnd("Importing Finish. MongoDB",file_size)
    sendToMailPsql.sendMailFinishMongo(toMail,str(time_difference),limit,pipeline)
    print("Importing data rows count : ",limit)

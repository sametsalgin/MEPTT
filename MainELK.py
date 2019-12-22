#UGUR INCI
from elasticsearch import Elasticsearch
import os
import json
import pandas
import csv
import platform#get pcname
import socket#get pcname
import xml.etree.cElementTree as ET
from timeit import default_timer
import datetime
import smtplib
import requests
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import sys
def main():
    while True:
        filename=input("Enter file name : ")
        fileif=os.path.exists(filename)
        if fileif==True:
            break
        if fileif==False:
            print("File not exist.")
            writeErrorLogFileELK("File not exist.")
    while True:
        select=input("Do you want to enter table name ?(Y/N) : ")
        if (select=="Y")or(select=="y"):
            tablename=input("Enter table name((lowercase) otherwise it will be converted to lowercase) : ")
            tablename=tablename.lower()
            break
        elif (select=="N")or(select=="n"):
            tablename="tablename"
            break
        else:
            print("Wrong choice")
            writeErrorLogFileELK("Wrong table name choice")
    while True:
        headerselect=input("Will table have header ?(1/0) : ")
        if headerselect=="1":
            break
        elif headerselect=="0":
            print("You must enter header otherwise you can't continue")
        else:
            print("Wrong choice")
            writeErrorLogFileELK("Wrong table header choice")
    start = default_timer()
    es=Elasticsearch([{'host':'localhost','port':9200}])
    cdir = os.path.dirname(__file__)
    file_res = os.path.join(cdir,filename)
    data = pandas.read_csv(file_res,encoding="UTF")
    colarray=[]
    while True:
        selectcol=input("Do you want to transfer ALL columns or a SOME column ?(A/S) : ")
        if (selectcol=="A")or(selectcol=="a"):
            data.rename(columns={"_id":"_idd"},inplace=True)
            data_json = json.loads(data.to_json(orient='records'))
            break
        elif (selectcol=="S")or(selectcol=="s"):
            while True:
                for x in data.columns:
                    print(x)
                while True:
                    columnsnum=input("Enter Number of columns : ")
                    if (columnsnum.isnumeric()==True)and(int(columnsnum)<=len(data.columns))and(int(columnsnum)>=1):
                        break
                    else:
                        print("Wrong choice")
                        writeErrorLogFileELK("Wrong number of columns choice")
                y=0
                columnsnum=int(columnsnum)
                while y<columnsnum:
                    colname=input("Enter column name : ")
                    if colname in data.columns:
                        colarray.append(colname)
                        y+=1
                    else:
                        print("Wrong choice")
                        writeErrorLogFileELK("Wrong column name choice")
                break
            data2=pandas.read_csv(file_res,usecols=colarray)
            data2.rename(columns={"_id":"_idd"},inplace=True)
            data_json = json.loads(data2.to_json(orient='records'))
            break
        else:
            print("Wrong choice")
            writeErrorLogFileELK("Wrong all columns or a some column choice")
    mailaddress=input("Enter your mail address : ")
    sendMailStart(mailaddress)
    x=0
    for d in data_json:
        x+=1
        res=es.index(index=tablename,doc_type='csv',id=x,body=d)
    print(x,"line")
    duration=default_timer()-start
    print("Duration :",duration)
    lineduration=duration/x
    print("Transfer time for each line :",lineduration)
    linepersecond=x/duration
    print("Lines per second :",linepersecond)
    runtime=datetime.datetime.now()
    print("Run time :",runtime)
    writeXMLELK(tablename,runtime,x,duration,lineduration,linepersecond)
    sendMailFinish(mailaddress,x,duration,lineduration,runtime,linepersecond)

def writeXMLELK(tableName,runTime,line,duration,lineduration,linepersecond):
    #xml code start
    pcName=os.environ['COMPUTERNAME']  #get pcname
    root = ET.Element("root")
    doc = ET.SubElement(root, "doc")

    ET.SubElement(doc,"field1", name="pcName").text = pcName
    ET.SubElement(doc,"field2", name="tableName").text = tableName
    ET.SubElement(doc,"field3", name="runTime").text = str(runTime)
    ET.SubElement(doc,"field4", name="line").text = str(line)
    ET.SubElement(doc,"field5", name="duration").text = str(duration)
    ET.SubElement(doc,"field6", name="lineduration").text = str(lineduration)
    ET.SubElement(doc,"field7", name="linespersecond").text = str(linepersecond)

    tree = ET.ElementTree(root)
    tree.write("filenameELK.xml")
    #xml code finish

def sendMailStart(toMail):
    message=MIMEMultipart()

    message["From"]="mep.tt.esogu@gmail.com"
    message["To"]="""%s"""%(toMail)
    message["Subject"]="KORLOG DATABASE PROCESS"

    text= """This e-mail has been sent to you by MEPTT.\nElasticsearch\nTransfer Started\n""" 

    message_text= MIMEText(text,"plain")
    message.attach(message_text)

    try:
        mail=smtplib.SMTP("smtp.gmail.com",587)
        mail.ehlo()
        mail.starttls()
        mail.login("mep.tt.esogu@gmail.com","ahmetyazici")
        mail.sendmail(message["From"],message["To"],message.as_string())
        print("Mail Sent")
        mail.close()
    except:
        sys.stderr.write("Mail Failed!")
        sys.stderr.flush()

def sendMailFinish(toMail,rowCountText,timeText,lineduration,runtime,linepersecond):
    message=MIMEMultipart()

    message["From"]="mep.tt.esogu@gmail.com"
    message["To"]="""%s"""%(toMail)
    message["Subject"]="KORLOG DATABASE PROCESS"

    text= """This e-mail has been sent to you by MEPTT.\nElasticsearch\nTransfer Finished\n%s line\nDuration : %s\nTransfer time for each line : %s\nRun time : %s\nLines per second :%s""" %(rowCountText,timeText,lineduration,runtime,linepersecond)

    message_text= MIMEText(text,"plain")
    message.attach(message_text)

    try:
        mail=smtplib.SMTP("smtp.gmail.com",587)
        mail.ehlo()
        mail.starttls()
        mail.login("mep.tt.esogu@gmail.com","ahmetyazici")
        mail.sendmail(message["From"],message["To"],message.as_string())
        print("Mail Sent")
        mail.close()
    except:
        sys.stderr.write("Mail Failed!")
        sys.stderr.flush()

def writeErrorLogFileELK(errors):
    now=datetime.datetime.today()
    datetime_now = now.strftime("%d/%m/%Y %H:%M:%S")
    opendFile=open("ErrorsLogRecordsElasticsearch.txt","a",encoding="utf")
    opendFile.write(errors+" "+datetime_now+"\n")

import os
import xml.etree.cElementTree as ET#for xml
def writeXML(dbName,tableName,runTime,user,password,host,port):
    #xml code start
    pcName=os.environ['COMPUTERNAME']  #get pcname
    root = ET.Element("root")
    doc = ET.SubElement(root, "doc")

    ET.SubElement(doc,"field1", name="pcName").text = pcName
    ET.SubElement(doc,"field2", name="dbName").text = dbName
    ET.SubElement(doc,"field3", name="user").text = user
    ET.SubElement(doc,"field4", name="password").text = password
    ET.SubElement(doc,"field5", name="host").text = host
    ET.SubElement(doc,"field6", name="port").text = port
    ET.SubElement(doc,"field7", name="tableName").text = tableName
    ET.SubElement(doc,"field8", name="runTime").text = str(runTime)

    tree = ET.ElementTree(root)
    tree.write("postgreSqlDb.xml")
    #xml code finish
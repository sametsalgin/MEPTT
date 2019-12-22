import smtplib
import requests
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import sys
#toMail=gonderilecek mail adresi
#dbname= database adı
#timetext=işlem süresini
#rowcounttext=satır sayısını
def sendMailStart(toMail):
    message=MIMEMultipart()

    message["From"]="mep.tt.esogu@gmail.com"
    message["To"]="""%s"""%(toMail)
    message["Subject"]="KORLOG DATABASE PROCESS"

    text= """This e-mail has been sent to you by MEPTT.\nPostgreSQL\nTransfer Started\n""" 

    message_text= MIMEText(text,"plain")
    message.attach(message_text)

    try:
        mail=smtplib.SMTP("smtp.gmail.com",587)
        mail.ehlo()
        mail.starttls()
        mail.login("mep.tt.esogu@gmail.com","ahmetyazici")
        mail.sendmail(message["From"],message["To"],message.as_string())
        print("Mail Sent!\n")
        mail.close()
    except:
        sys.stderr.write("Mail Failed!\n")
        sys.stderr.flush()


def sendMailFinish(toMail,timeText,rowCountText,lineDuration):
    message=MIMEMultipart()

    message["From"]="mep.tt.esogu@gmail.com"
    message["To"]="""%s"""%(toMail)
    message["Subject"]="KORLOG DATABASE PROCESS"

    text= """This e-mail has been sent to you by MEPTT.\nPostgreSQL\nTransfer Finished\nProcess Time= %s\nRow Count: %s\nTransfer time for each line: %s\n""" %(timeText,rowCountText,lineDuration)

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
        sys.stderr.write("Mail Failed!\n")
        sys.stderr.flush()

def sendMailStartMongo(toMail):
    message=MIMEMultipart()

    message["From"]="mep.tt.esogu@gmail.com"
    message["To"]="""%s"""%(toMail)
    message["Subject"]="KORLOG DATABASE PROCESS"

    text= """This e-mail has been sent to you by MEPTT.\nMongoDB\nTransfer Started\n""" 

    message_text= MIMEText(text,"plain")
    message.attach(message_text)

    try:
        mail=smtplib.SMTP("smtp.gmail.com",587)
        mail.ehlo()
        mail.starttls()
        mail.login("mep.tt.esogu@gmail.com","ahmetyazici")
        mail.sendmail(message["From"],message["To"],message.as_string())
        print("Mail Sent!\n")
        mail.close()
    except:
        sys.stderr.write("Mail Failed!\n")
        sys.stderr.flush()


def sendMailFinishMongo(toMail,timeText,row_count,mb_s):
    message=MIMEMultipart()

    message["From"]="mep.tt.esogu@gmail.com"
    message["To"]="""%s"""%(toMail)
    message["Subject"]="KORLOG DATABASE PROCESS"

    text= """This e-mail has been sent to you by MEPTT.\nMongoDB\nTransfer Finished\nProcess Time= %s\nRow Count: %s\nTransfer MB/S: %s\n""" %(timeText,row_count,mb_s)

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
        sys.stderr.write("Mail Failed!\n")
        sys.stderr.flush()

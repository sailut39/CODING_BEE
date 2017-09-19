#!/opt/anaconda3/bin/python3.5
'''
Created on Feb 22, 2017

@author: Sailendra Tata

> Put all data source ids in dsid.dat-> braces shoudl exist and each data source id should delimited by ,
> email addition should be done at 2 places - msg['To']  and send email
> execution command  is ./<scriptname>

'''

import logging
import datetime
from datetime import timedelta
import teradata
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

date1=str(datetime.date.today())
LOG_FILENAME = "example-"+date1+".log"
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG,filemode='w')
logging.info("LOG_FILENAME %s"%LOG_FILENAME)
logging.debug("message to log file")
logging.info("date is  %s"%date1)

udaExec = teradata.UdaExec (appName="log_sender", version="1.0",
        logConsole=False)
session = udaExec.connect(method="ODBC", system="XXX.XX.XXX.XX",
        username="scott", password="XXXXXXX");
dsid_file="/home/stata001c/dsid.dat"
dsid = open(dsid_file, 'r').read()

QUERY = """\
SELECT '<tr> <td>'|| cntrl.data_source_id || '</td> <td>' || COALESCE(ref.data_source_owner,'NA') || '</td> <td>' \
|| CAST(cntrl.incremental_load_start_ts AS VARCHAR(26) ) || '</td> <td>' || \
COALESCE(CAST(((cntrl.batch_end_ts - cntrl.batch_start_ts)  HOUR TO MINUTE ) AS VARCHAR(10)),'NA') || \
 '</td> <td><span style="background-color:' || \
CASE WHEN cntrl.batch_status_cd = 'C' THEN ' #27AE60;">'\
     WHEN cntrl.batch_status_cd = 'N' THEN ' #E74C3C;">' \
    ELSE ' #F1C40F;">' END || cntrl.batch_status_cd || '</td> </tr> ' \
FROM NDW_FUSION_UAT_PRCS_CNTRL_VIEWS.NDW_ETL_BATCH_CONTROL cntrl INNER JOIN ndw_prcs_cntrl_views.ndw_data_source_ref ref \
 ON cntrl.data_source_id = ref.data_source_id WHERE cntrl.data_source_id \
IN %s 
 and cntrl.incremental_load_start_ts(date)='%s'
"""%(dsid,str(datetime.date.today() - timedelta(days=1)))

#print ("QUERY is - %s" %QUERY)

#file_nm ="C:\Users\stata001c\Desktop\Python\sailu.html"

file_nm ="/home/stata001c/sailu.html"

text_file = open(file_nm, "w")
text_file.write("""\
<html>\
<p>Here is the staus of UAT loads for %s, all these stats were collected on 8:00 AM EST, GREEN represents completed, RED represents not yet triggered and YELLOW for running jobs, </p>
<table style="height: 126px; margin-left: auto; margin-right: auto;" border="1" width="477">\
<tbody>\
<tr>\
<td><strong>Dsid</strong></td>\
<td><strong>Inteface_nm</strong></td>\
<td><strong>Inc_start_ts</strong></td>\
<td><strong>Exec time</strong></td>\
<td><strong>Status</strong></td>\
</tr>\
"""%str(datetime.date.today()))

text_file.write("\n")
text_file.close()

for row in session.execute(QUERY):
#    print row[0]
    text_file = open(file_nm, "a")
    text_file.write(row[0])
    text_file.write("\n")
    text_file.close()

text_file = open(file_nm, "a")
text_file.write("""\
</tbody> \
</table> \
<p>Thanks, </p>
<p>Furious team</p>
</html>""")
text_file.write("\n")
text_file.close()


html = open(file_nm, 'r').read()

# email functionality starts here

body_text = "Here is the status of UAT loads on %s , please find GREEN are completed, red are not yet triggered and YELLOW for running jobs,"%str(datetime.date.today())
signature = "Thanks, \
Furious team\
"
#msg1 = MIMEText(body_text,'plain')
msg2 = MIMEText(html,'html')
#msg3 = MIMEText(body_text,'plain')

msg = MIMEMultipart('alternative')
msg['Subject'] = '<*******test email*******>NDW-FUSION UAT Load status'
msg['From'] = 'sailendra_tata@email.com'
msg['To'] = 'sailendra_tata@email.com'

#msg.attach(msg1)
msg.attach(msg2)
#msg.attach(msg3)

s = smtplib.SMTP('localhost')

s.sendmail('sailendra_tata@email.com', ['sailendra_tata@email.com','[DL@email.com]'], msg.as_string())
s.quit()

session.close()

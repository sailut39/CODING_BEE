'''
Created on Jun 9, 2017
Usage:
<script name> table_id time_slot_dtls 
@author: Sailendra Tata
Requirement: 
Create a script to check for source load status before copying data to target,
my requirement says, source has another reference table to tell about actual table load status, 
                     reference table holds the details along with a short table name,
                     each table will loads in 16 time slots(form 14 markets) and all have individual entries on reference table,
                     my ETL design will assign a table id for each target table, so we refer target table with help of table_id
                     This design is capable to hold until 3 hrs and at each 10 mins it will verify again with source for successful status

Usage- <script_name> table_id timeslot ref_tbl_nm short_tbl_nm
example:
python NRT_DEPENDANCY_CHECK.py 4614 4 nrt_daily_status NRTPMTT
'''
import glob,os
import sys,string
from pickle import NONE
import ConfigParser
import cx_Oracle
import time

#***Sys.argv represents the command line arguements being passed#
#***************************************************************#    
inifile=glob.glob("/etc/se/difa/%s*"%sys.argv[1])[0] # we hold all required variables in a separate ini file
timeslot=sys.argv[2]
ref_tbl_nm=sys.argv[3]
short_tbl_nm=sys.argv[4].upper()

# script level limits for looping times, measuring in seconds
max_sleep_time=1800
sleep_interval=600
sleep_var=0
waiting_time=0

# read ini file to fetch variables
cfg = ConfigParser.ConfigParser()
cfg.read(inifile)
#print (inifile)

DB_USER_NAME=cfg.get('GLOBAL','SOURCE_DB_USER')
DB_PASSWORD=cfg.get('GLOBAL','SOURCE_DB_PASSWORD')
DB_HOSTNAME=cfg.get('GLOBAL','SOURCE_DB_HOST_NM')
DB_PORT=cfg.get('GLOBAL','SOURCE_DB_PORT')
DB_SERVICE_NAME=cfg.get('GLOBAL','SOURCE_DB_NM')
DAY_ID=cfg.get('GLOBAL','DAY_ID')               # this value will change for each run in a day


sql_query="select * from \
(select a.process_date From MKT1."+ref_tbl_nm +" a where table_name=\'"+ short_tbl_nm + "\' union all\
 select a.process_date From MKT2."+ref_tbl_nm +" a where table_name=\'"+ short_tbl_nm + "\' union all\
 select a.process_date From MKT3."+ref_tbl_nm +" a where table_name=\'"+ short_tbl_nm + "\' union all\
 select a.process_date From MKT4."+ref_tbl_nm +" a where table_name=\'"+ short_tbl_nm + "\' union all\
 select a.process_date From MKT5."+ref_tbl_nm +" a where table_name=\'"+ short_tbl_nm + "\' union all\
 select a.process_date From MKT6."+ref_tbl_nm +" a where table_name=\'"+ short_tbl_nm + "\' union all\
 select a.process_date From MKT7."+ref_tbl_nm +" a where table_name=\'"+ short_tbl_nm + "\' union all\
 select a.process_date From MKT8."+ref_tbl_nm +" a where table_name=\'"+ short_tbl_nm + "\' union all\
 select a.process_date From MKT9."+ref_tbl_nm +" a where table_name=\'"+ short_tbl_nm + "\' union all\
 select a.process_date From MKT10."+ref_tbl_nm +" a where table_name=\'"+ short_tbl_nm + "\' union all\
 select a.process_date From MKT11."+ref_tbl_nm +" a where table_name=\'"+ short_tbl_nm + "\' union all\
 select a.process_date From MKT12."+ref_tbl_nm +" a where table_name=\'"+ short_tbl_nm + "\' union all\
 select a.process_date From MKT13."+ref_tbl_nm +" a where table_name=\'"+ short_tbl_nm + "\' union all\
 select a.process_date From MKT14."+ref_tbl_nm +" a where table_name=\'"+ short_tbl_nm + "\' union all\
 select a.process_date From MKT15."+ref_tbl_nm +" a where table_name=\'"+ short_tbl_nm + "\') where process_date= '%s' \
 and PULL_TME_SLOT = %s"%(DAY_ID,timeslot.strip(',.- '))

print ("query prepared to hit source*********** %s"%(sql_query))
# construct string for connections
connection_string_vantage="%s/%s@%s:%s/%s"%(DB_USER_NAME,DB_PASSWORD.strip('\''),DB_HOSTNAME,DB_PORT,DB_SERVICE_NAME)
conn = cx_Oracle.connect(connection_string_vantage)
print (conn.version)
# defining a variable and setting it to null, it will be useful later to hold result set from oracle
passedparameter=NONE

conn = cx_Oracle.connect(connection_string_vantage)
curs=conn.cursor()
print ('Connection established with VANTAGE source')

print ("Data pulled from source \n\
_________________________________________________\n\
|PROCESS_DATE|PULL_TME_SLOT|TABLE_NAME|MARKET_NM|\n\
_________________________________________________")


def check_Vtg_Status():
    curs.execute(sql_query)
    global passedparameter
    passedparameter=curs.fetchall()
    for i in passedparameter:
#        print (str(i[0])+'|'+str(i[1])+'|'+str(i[2])+'|'+str(i[3]))
        print('|' + str(i[0]).ljust(12,' ') + '|' + str(i[1]).center(13,' ') + '|' + str(i[2]).ljust(10,' ') + '|' + str(i[3]).ljust(10,' ')+'|')    
    print ("_________________________________________________")
    print ("length of output from source:" + str(len(passedparameter)))


# call the function to check status for given information
check_Vtg_Status()

if len(passedparameter) == 15:
    status_flag=1
    print ("everything looks good, exiting from code")
    conn.close()
    sys.exit(0)
else:
    # resetting the result set variable
    del passedparameter
    print ("no load progress on source system,going into sleep")
    i=1
    while (waiting_time <= max_sleep_time):
        print ("waiting started, mins left %s")%(str((max_sleep_time-(sleep_interval*i))/60))
        time.sleep(sleep_interval)
        i+=1
        waiting_time=sleep_interval*i
        check_Vtg_Status()
        if len(passedparameter) == 15:
            status_flag=1
            print ("everything looks good, exiting!!")
            conn.close()
            sys.exit(0)
print("waiting period has reached to max limit, exiting from process with no error code, \
we are going ahead to pull what ever the data available on SOURCE, extracted records  may not be latest data")
conn.close()
sys.exit(0)


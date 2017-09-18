'''
Created on Sep 18, 2017

@author: Sailendra Tata

This Python script will generate a TPT script based on Teradata table definition

dName = Use your project specific database name where actual table resides
tName =    actual table name
fPath = "eplace correct directory
errDbNm =  Schema for error database, use it as per project environment
session = Use proper IP, Username and passwords
TdpId = # USe proper tdpID as per your environment

'''


import teradata

# Variable declarations and definitions for few...

attrValues = []
dummy = []
dName = "tata" #Use your project specific database name where actual table resides
tName = "employee"   # actual table name
fPath = "<your-actual-path>"+tName+".lst...tmp.lst" # replace correct directory
errDbNm = "tata_err"  # Schema for error database, use it as per project environment
ins_sec = []
ins_attrs = []
ins_values = []
metaDataFile = tName.upper()+"_stg.tpt"

def colNmParser(colDataTypeNm,colLen,colFormate):
    colNmStr = str(colDataTypeNm).strip()
    colLenStr = str(colLen).strip()
    colFormateStr = str(colFormate).strip()
    if (colNmStr == "CF"):
        return "CHAR("+colLenStr+")"
    if (colNmStr == "CV"):
        return "VARCHAR("+colLenStr+")"
    if (colNmStr == "TS" and colLenStr == "19" ):
        return "TIMESTAMP(0) FORMAT "+ colFormateStr 
    if (colNmStr == "TS" and colLenStr == "23" ):
        return "TIMESTAMP(3) FORMAT "+ colFormateStr
    if (colNmStr == "TS" and colLenStr == "26" ):
        return "TIMESTAMP(6) FORMAT "+ colFormateStr
    if (colNmStr == "N"):
        return "NUMBER("+colLenStr+")"
    if (colNmStr == "AT"):
        return "TIME"
    if (colNmStr == "BF"):
        return "BYTE"
    if (colNmStr == "BV"):
        return "VARBYTE"
    if (colNmStr == "D"):
        return "DECIMAL"
    if (colNmStr == "DA"):
        return "DATE"
    if (colNmStr == "F"):
        return "FLOAT"
    if (colNmStr == "I"):
        return "INTEGER"
    if (colNmStr == "I1"):
        return "BYTEINT"
    if (colNmStr == "I2"):
        return "SMALLINT"
    if (colNmStr == "I8"):
        return "BIGINT"
    else:
        return None
 
def getDefBlock(dataBaseName,tableName):
    query = "SELECT columnname,columntype,columnlength,columnformat from dbc.columns where tablename ='%s' and databasename = '%s'" % (tableName,dataBaseName)
    udaExec = teradata.UdaExec (appName="TPT generator", version="1.0",logConsole=False)
    session = udaExec.connect(method="odbc", system="xx.xx.xxx.xxx",username="scott", password="tiger"); # use proper logons as per your load user
    for row in session.execute(query):
        dummy.append(str(row[0]).strip()+" VARCHAR(5000)")
    thefile = open(metaDataFile, 'w')
    lenDummy = len(dummy) - 1
    thefile.write("DEFINE JOB FILE_LOAD \n DESCRIPTION 'Load a Teradata table from a file' \n (DEFINE SCHEMA Stage_Schema( \n")
    for j,i in enumerate(dummy):
        if j == lenDummy:
            thefile.write(i+");\n\n\n")
        else:
            thefile.write("\t"+i+",\n")
    thefile.close()

def file_ReaderBlock(tableName,filePath):
    dummy = []
    dummy.append("\n\n")
    dummy.append("DEFINE OPERATOR FILE_READER \n TYPE DATACONNECTOR PRODUCER \n SCHEMA Stage_Schema \n ATTRIBUTES \n(\n\
            VARCHAR PrivateLogName = '%s.log',\n\
            VARCHAR FileList = 'Y',\n\
            VARCHAR FileName = '%s',\n\
            VARCHAR Format = 'Delimited',\n\
            VARCHAR OpenMode = 'Read',\n\
            INTEGER SkipRows = 0,\n\
            VARCHAR TextDelimiter ='$^$',\n\
            VARCHAR QUOTED = 'Optional', \n\
            VARCHAR EscapeQuoteDelimiter = '\\'); \n\n \
DEFINE OPERATOR DDL_OPERATOR() \n\
DESCRIPTION 'TERADATA PARALLEL TRANSPORTER DDL OPERATOR'\n\
TYPE DDL \n ATTRIBUTES \n( \n\
            VARCHAR PrivateLogName = '%s_ddloper.log', \n\
            VARCHAR TdpId          = 'XXXX', \n\            # USe proper tdpID as per your environment
            VARCHAR UserName              = @UsrID, \n\
            VARCHAR UserPassword   = @Pwd, \n\
            VARCHAR ErrorList      = '3807' \n\
); \n" % (tableName,filePath,tableName))
    thefile = open(metaDataFile, 'a')
    for i in dummy:
        thefile.write(i+"\n")
    thefile.close()

def error_log(errDbNm,tableName,tgtDbNm):  
    dummy = []
    dummy.append("STEP Drop_Error_Log_Tables \n ( \n APPLY \n\
            ('DROP TABLE '  || '%s'  || '.%s_LT;'), \n\
            ('DROP TABLE '  || '%s'  || '.%s_ET;'), \n\
            ('DROP TABLE '  || '%s'  || '.%s_UV;'), \n\
            ('DROP TABLE '  || '%s'  || '.%s_WT;') \n\
            TO OPERATOR (DDL_OPERATOR() ); \n ) ;\n\
DEFINE OPERATOR LOAD_OPERATOR \n TYPE UPDATE \n SCHEMA * \n ATTRIBUTES \n( \n\
          VARCHAR PrivateLogName = '%s_load.log', \n\
          INTEGER MaxSessions = 20, \n\
          INTEGER MinSessions = 1, \n\
          INTEGER SkipRows = 0, \n\
          VARCHAR TdpId = 'COMSTG', \n\
          VARCHAR UserName = @UsrID, \n\
          VARCHAR UserPassword = @Pwd, \n\
          VARCHAR TargetTable = '%s' || '.'||'%s', \n\
          VARCHAR LogTable         = '%s'|| '.'||'%s'||'_LT',\n\
          VARCHAR WorkTable      = '%s'|| '.'||'%s'||'_WT',\n\
          VARCHAR ErrorTable1 = '%s' || '.'||'%s'||'_ET',\n\
          VARCHAR ErrorTable2 = '%s' || '.'||'%s'||'_UV' \n );" %(errDbNm, tableName,errDbNm, tableName,errDbNm, tableName,errDbNm, tableName,tableName,tgtDbNm,tableName,errDbNm, tableName,errDbNm, tableName,errDbNm, tableName,errDbNm, tableName))
    thefile = open(metaDataFile, 'a')
    for i in dummy:
        thefile.write(i+"\n")
    thefile.close()    

def ins_Section_Cols(dataBaseName, tableName):
    query = "SELECT columnname,columntype,columnlength,columnformat from dbc.columns where tablename ='%s' and databasename = '%s'" % (tableName,dataBaseName)
    udaExec = teradata.UdaExec (appName="TPT generator Load section", version="1.0",logConsole=False)
    session = udaExec.connect(method="odbc", system="xx.xx.xxx.xxx",username="scott", password="tiger"); # use proper logons as per your load user
    for row in session.execute(query):
        ins_sec.append((str(row[0]).strip()+" "+colNmParser(row[1],row[2],row[3])).split())
    session.close()
    ins_sec_len = len(ins_sec)
    ins_attrs.append("STEP Load_" +tableName+ "_Table \n( \nAPPLY \n( \n    'INSERT INTO ' || '"+ dataBaseName +"' || '."+ tableName +" ( \n")
    for i,j in enumerate(ins_sec):
        if i == ins_sec_len - 1:
            ins_attrs.append(j[0] + ") \n VALUES \n ( \n")
            ins_values.append("CAST(:%s AS %s) \n ); \n )" %(j[0],j[1]))
        else:
            ins_attrs.append(j[0]+",")
            ins_values.append("CAST(:%s AS %s)," %(j[0],j[1]))

    thefile = open(metaDataFile, 'a')
    for i in ins_attrs:
        thefile.write("\t"+i+"\n")
    for i in ins_values:
        thefile.write("\t"+i+"\n")
    thefile.close()

def final_Block():
    thefile = open(metaDataFile, 'a')
    thefile.write("TO OPERATOR (LOAD_OPERATOR[1]) \n    SELECT ")
    ins_sec_len = len(ins_sec)
    for i,j in enumerate(ins_sec):
        if i == ins_sec_len - 1:
            thefile.write(j[0] + " FROM OPERATOR(FILE_READER[1]); \n); \n);")
        else:
            thefile.write(j[0]+" , ")
    thefile.close()


# Main code execution will starts here
getDefBlock(dName,tName)
file_ReaderBlock(tName.lower(),fPath)
error_log(errDbNm,tName,dName)
ins_Section_Cols(dName,tName)
final_Block()

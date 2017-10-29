#!/usr/bin/ksh
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# <Script> <DATA_LAKE/TD_DW> <UAT/NA> EXPORT_DATA_LAKE_PRCS_ID EXPORT_DATA_LAKE_TBL_ID TD_DW_SOURCE_ID1,TD_DW_SOURCE_ID2, .. \
#  > 
# example commands-
# ksh /home/sailendra/TD_DW_DATA_LAKE_depandancy_chk.sh DATA_LAKE NA 3338 3203 1341
# ksh /data/infa_shared/Scripts/Shared/TD_DW_DATA_LAKE_depandancy_chk.sh TD_DW NA 547 547 1341
# Desc- it will check the dependency of DATA_LAKE data export jobs and TD_DW load jobs, vice versa applicable 
# Author: SAILENDRA
# Modification history:- 
#                          11/09/2016 - Initial version
#
#
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
### set environment variables
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/opt/oracle/client-11.2.0/lib:/opt/teradata/client/14.10/lib64:/opt/teradata/client/14.10/tbuild/lib64:/bin:/opt/teradata/client/14.10/tdicu/lib:
export TNS_ADMIN=/opt/oracle/client-11.2.0/network/admin
export PATH=${PATH}:/opt/teradata/client/14.10/tbuild/bin:/usr/local/bin:/bin:/usr/bin:/opt/oracle/client-11.2.0/bin:/opt/teradata/client/14.10/bin:
export ORACLE_LIB=/opt/oracle/client-11.2.0/lib
export ORACLE_HOME=/opt/oracle/client-11.2.0
### capture all command line parameters
export ENVIRONMENT=${1}
export RUN_ENV=${2}        ### UAT for UAT environment.. NA for all others
export DATA_LAKE_PRCS_ID=${3}
export DATA_LAKE_TBL_ID=${4}
export TD_DW_SOURCE_ID=${5}
export PRCS_CNTRL_VIEWS="TD_DW_REF_VIEWS"
### Variables for sleep
export SLEEP_TIME=3600
export INTERVAL_TIME=300
export LOOP_TIME=0

### validate the command line arguments
if [ $# -ne 5 ]
then
echo "@@@@@- number of arguments are wrong, please check and pass correct args"
exit 3
fi

if [ -z ${ENVIRONMENT} ] || [ -z ${RUN_ENV} ] || [ -z ${DATA_LAKE_PRCS_ID} ] || [ -z ${DATA_LAKE_TBL_ID} ] || [ -z ${TD_DW_SOURCE_ID} ]
then
echo "@@@@@- in correct arguments passed, please check and retry"
exit 5
fi
### set process control for UAT environment - required only for UAT
if [ "${RUN_ENV}" = "UAT" ] || [ "${RUN_ENV}" = "uat" ]
then
export PRCS_CNTRL_VIEWS="TD_DW_UAT_REF_VIEWS"
fi

if [ "${ENVIRONMENT}" = "DATA_LAKE" ] || [ "${ENVIRONMENT}" = "data_lake" ] 
then
export LOG_FILE="/home/sailendra/logs/TD_DW_DATA_LAKE_depandancy_chk_${DATA_LAKE_TBL_ID}_`date "+%Y%m%d%H%M%S"`.log"
else
export LOG_FILE="/home/sailendra/TD_DW/SHELL_Logs/TD_DW_DATA_LAKE_depandancy_chk_${DATA_LAKE_TBL_ID}_`date "+%Y%m%d%H%M%S"`.log"
fi
echo "@@@@@- log file is : ${LOG_FILE}"

exec > ${LOG_FILE}
### Following if condition applicable for DATA_LAKE only

function DATA_LAKE_check 
{
export DATA_LAKE_INI="/etc/se/DATA_LAKE/"${DATA_LAKE_TBL_ID}"*.ini"
export USER_NAME=$(whoami)
export job_config="/home/sailendra/config/${USER_NAME}_config.ini"
export tmp_job_config="/tmp/${DATA_LAKE_TBL_ID}_DATA_LAKE.ini"
export temp_dir="/tmp"
rm -f ${tmp_job_config} ${temp_dir}/${4}_export.dat

grep "TARGET" ${DATA_LAKE_INI} > ${tmp_job_config}
. ${tmp_job_config}
. ${job_config}
export TD_LOGON=".LOGON ${TARGET_DB_HOST_NM}/${TARGET_DB_USER},${TARGET_DB_PASSWORD};"
export ORA_LOGON="${JOB_DB_USER_NAME}/$JOB_DB_PASSWORD@JOBCTL"

### Get max end date for recent completed record from TD_DW - status code 'C'

bteq << EOF
${TD_LOGON}
.SET TITLEDASHES OFF;
.SET WIDTH 100;
.SET TIMEMSG NONE;
.SET SEPARATOR '|'
.SET RECORDMODE OFF;
.EXPORT FILE = ${temp_dir}/${4}_export.dat;
SELECT TD_DW_SOURCE_ID (title ''),CAST(MAX(INCREMENTAL_LOAD_START_TS) AS DATE FORMAT 'YYYY-MM-DD') (title '') FROM ${PRCS_CNTRL_VIEWS}.TD_DW_ETL_BATCH_CONTROL WHERE TD_DW_SOURCE_ID IN (${TD_DW_SOURCE_ID}) AND BATCH_STATUS_CD = 'N' GROUP BY 1;
. EXIT;
EOF

### Get max start date for recent run from DATA_LAKE ORACLE - status code 'N'

export START_DATE_DATA_LAKE=$(sqlplus -s ${ORA_LOGON} << !!
       set pagesize 0;
	   set feedback off; 
	   set verify off;
	   set heading off;
	   set echo off;
       SELECT TO_CHAR(MAX(INCREMENTAL_LOAD_START_TS),'YYYY-MM-DD') FROM _SCHEMA.DATA_LAKE_LOAD_REF WHERE DL_PRCS_ID=${DATA_LAKE_PRCS_ID} AND PRCS_STATUS='N';
       quit;
!!
)

### checking if any TD_DW_SOURCE_ids on TD_DW have different dates - expecting all should have same date

export MIS_MATCH=$(sed "1d" ${temp_dir}/${4}_export.dat | cut -d "|" -f2 | uniq -c | wc -l)

### getting TD_DW end date for comparison purposes

export TD_DW_START_DATE=$(tail -1 ${temp_dir}/${4}_export.dat | cut -d "|" -f2)

echo "@@@@@- TD_DW_START_DATE - ${TD_DW_START_DATE}"
echo "@@@@@- START_DATE_DATA_LAKE - ${START_DATE_DATA_LAKE}"


if [ ${MIS_MATCH} -ne 1 ]
then
echo "@@@@@- something wrong with TD_DW jobs.. please check and clear off them before restart"
exit 1
fi

}

### Following if condition applicable for TD_DW only

function TD_DW_check
{

### initialize TD_DW global vars
. /home/sailendra/TD_DW/Scripts/.TD_DW.env

get_pwd TD_DW_DATA_LAKE_LOAD 				### it will automatically set up TDLOGONSTR with required format
export ORA_LOGON=$(cat ${TDLOGONDIR}/../TD_Logon/DATA_LAKE_read_password) ### get DATA_LAKE oracle logons
temp_dir="/tmp"

rm -f ${tmp_job_config} ${temp_dir}/${4}_export.dat


### Get max start date for recent completed record from TD_DW - status code 'N'

bteq << EOF
${TDLOGONSTR}
.SET TITLEDASHES OFF;
.SET WIDTH 100;
.SET TIMEMSG NONE;
.SET SEPARATOR '|'
.SET RECORDMODE OFF;
.EXPORT FILE = ${temp_dir}/${4}_export.dat;
SELECT TD_DW_SOURCE_ID (title ''),CAST(MAX(INCREMENTAL_LOAD_START_TS) AS DATE FORMAT 'YYYY-MM-DD') (title '') FROM ${PRCS_CNTRL_VIEWS}.TD_DW_ETL_BATCH_CONTROL WHERE TD_DW_SOURCE_ID IN (${TD_DW_SOURCE_ID}) AND BATCH_STATUS_CD = 'N' GROUP BY 1;
. EXIT;
EOF

export TD_DW_START_DATE=$(tail -1 ${temp_dir}/${4}_export.dat | cut -d "|" -f2)

### Get max start date for recent run from DATA_LAKE ORACLE - status code 'C'

export START_DATE_DATA_LAKE=$(sqlplus -s ${ORA_LOGON} << !!
       set pagesize 0;
	   set feedback off; 
	   set verify off;
	   set heading off;
	   set echo off;
       SELECT TO_CHAR(INCREMENTAL_LOAD_START_TS,'YYYY-MM-DD') FROM DATA_LAKE.PROCESS_CONTROL WHERE PRCS_ID=${DATA_LAKE_PRCS_ID} AND PRCS_STATUS='C' AND TO_CHAR(INCREMENTAL_LOAD_START_TS,'YYYY-MM-DD') = '${TD_DW_START_DATE}';
       quit;
!!
)

### checking if any TD_DW_SOURCE_ids on TD_DW have different dates - expecting all should have same date

 export MIS_MATCH=$(sed "1d" ${temp_dir}/${4}_export.dat | cut -d "|" -f2 | uniq -c | wc -l)

### getting TD_DW end date for comparison purposes

echo "@@@@@- TD_DW_START_DATE - ${TD_DW_START_DATE}"
echo "@@@@@- START_DATE_DATA_LAKE - ${START_DATE_DATA_LAKE}"

if [ ${MIS_MATCH} -ne 1 ] 
then
echo "@@@@@- something wrong with TD_DW jobs.. please check and clear off them before restart"
exit 11
fi

}

# TD_DW check
if [ "${ENVIRONMENT}" = "TD_DW" ] || [ "${ENVIRONMENT}" = "td_dw" ]    ### TD_DW related logics start
then
export USER_NAME=$(whoami)

	while [ ${LOOP_TIME} -lt ${SLEEP_TIME} ]; do
		TD_DW_check
		
		### check TD_DW dates and DATA_LAKE dates
		if [ "${START_DATE_DATA_LAKE}" = "${TD_DW_START_DATE}" ]
		then
			break
		else
			echo "@@@@@- Date on TD_DW ( ${TD_DW_START_DATE} ) and DATA_LAKE ( ${START_DATE_DATA_LAKE} ) are not matching, sleeping for 5 minutes to verify again on DATA_LAKE "
			sleep ${INTERVAL_TIME}
			LOOP_TIME=$(expr ${LOOP_TIME} + ${INTERVAL_TIME})
			continue
		fi
	done
	if [ "${START_DATE_DATA_LAKE}" = "${TD_DW_START_DATE}" ]
	then 
		echo "@@@@@- We are good to proceed with Base load data processing, @@@@@- Date on TD_DW ( ${TD_DW_START_DATE} ) and DATA_LAKE ( ${START_DATE_DATA_LAKE} ) are matching "
		exit 0
	else
		echo "@@@@@- Date on TD_DW ( ${TD_DW_START_DATE} ) and DATA_LAKE ( ${START_DATE_DATA_LAKE} ) are not matching, Max sleep time has reached,\
 ensure all runs are completed on DATA_LAKE and restart the process carefully "
		exit 9
	fi
fi

# DATA_LAKE check
if [ "${ENVIRONMENT}" = "DATA_LAKE" ] || [ "${ENVIRONMENT}" = "DATA_LAKE" ]   ### DATA_LAKE related logics start
then 


	while [ ${LOOP_TIME} -lt ${SLEEP_TIME} ]; do
		DATA_LAKE_check
		
		### check TD_DW dates and DATA_LAKE dates
		if [ "${START_DATE_DATA_LAKE}" = "${TD_DW_START_DATE}" ]
		then
			break
		else
			echo "@@@@@- Date on TD_DW ( ${TD_DW_START_DATE} ) and DATA_LAKE ( ${START_DATE_DATA_LAKE} ) are not matching, sleeping for 5 minutes to verify again on TD_DW "
			sleep ${INTERVAL_TIME}
			LOOP_TIME=$(expr ${LOOP_TIME} + ${INTERVAL_TIME})
			continue
		fi
	done

### check TD_DW dates and DATA_LAKE dates
	if [ "${START_DATE_DATA_LAKE}" = "${TD_DW_START_DATE}" ]
	then
		echo "@@@@@- We are good to proceed and continuing to export process"
		exit 0
	else
		echo "@@@@@- Date on TD_DW ( ${TD_DW_START_DATE} ) and DATA_LAKE ( ${START_DATE_DATA_LAKE} ) are not matching, please check the failures and restart the process "
		exit 2
	fi
fi       ### DATA_LAKE related logics- close


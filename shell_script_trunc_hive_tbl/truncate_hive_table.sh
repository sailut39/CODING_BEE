#!/bin/ksh
################################################################################################################
#Description: Script is designed to truncate stage data(complete) and TGT HIve table 
#             for past 15 days based on date from input, TGT table data will be moved to a safe path 
#             before sweeping it from actual hdfs path, safe path will provide transaction control mechanism 
#             in case of any failures with TGT table loads, safe path will holds only today's/yday's truncated data
#             No seperate log file will be maintained, 
#Usage      : sh <script_name> <table_id> 2> /tmp/truncate_hive_table.log
#Example    : sh truncate_hive_table.sh 5049 2> /tmp/truncate_hive_table.log
#Modification history:
#Initial version- Sailendra -06/29/17
################################################################################################################ 

# Fetch required variables from environment file
env_path='/home/stata001c/env'
DAY_ID=$(grep "^DAY_ID=" ${env_path}/$1_*.ini | cut -d "=" -f2)
TGT_DIR=$(grep "^TGT_DIR=" ${env_path}/$1_*.ini | cut -d "=" -f2)
SRC_TABLE_NAME=$(grep "^SOURCE_TABLE_NAME=" ${env_path}/$1_*.ini | cut -d "=" -f2 | awk '{print tolower($0)}')
STAGE_DIR=$(grep "^STG_DIR=" ${env_path}/$1_*.ini | cut -d "=" -f2 | awk '{print tolower($0)}')
echo "DAY_ID is $DAY_ID, TGT_DIR is ${TGT_DIR},Source table name is ${SRC_TABLE_NAME}"

# This function helps to revert back data to actual tgt table, so that data integrity will be maintained

exit_func () {
if [ $# -eq 2 ];then
	echo -e "\n++++ Reverting the moved data to actual tgt path and collecting latest stats on tgt table ++++ \n"
c=0
	while [ $c -le 15 ]
	do
	computed_date=$(date -d "${DAY_ID} -${c} days" +%Y-%m-%d)
	hdfs dfs -mv ${TGT_TRUNC_DATA_PATH}/day_id=${computed_date} ${TGT_DIR}/${SRC_TABLE_NAME}/day_id=${computed_date}
	c=`expr $c + 1`
	done

	hive -e "msck repair table TGT.my_table"
	exit $1
fi
	exit $1
}

if [ -z $DAY_ID ];
then
echo "Unable to get DAY_ID, please verify env file for DAY_ID"
exit 1
fi 

a=0

# Truncate data from stage table
# Define safe data path 
export TGT_TRUNC_DATA_PATH="/home/sailendra/my_table_trunc"

# check whether safe path data directory exist on hdfs, else create one right away
hdfs dfs -test -d ${TGT_TRUNC_DATA_PATH}

if [ $? -eq 0 ]; then
    echo "safe path directory exist on hdfs ${TGT_TRUNC_DATA_PATH} "
else
    echo "safe path direcroty does not exists, creating a new hdfs directory right away ${TGT_TRUNC_DATA_PATH}"
    hdfs dfs -mkdir -p ${TGT_TRUNC_DATA_PATH}
    if [ $? -ne 0 ]; then
    echo "something went wrong with safe path hdfs directory creation, existing the program ${TGT_TRUNC_DATA_PATH}"
    exit 3
    fi
fi

echo "Start safepath data clean up"

#Clean up safe path data
hdfs dfs -rm -R -f -skipTrash ${TGT_TRUNC_DATA_PATH}/*

echo "+++++++Journal table count before clean up"
hive -e "select count(*) from TGT.my_table"

echo "************ STARTING PARTITIONS MOVE ON HDFS FOR TGT TABLE ************"

# Loop start, it will dynamically calculates the date from day_id and clean up file system for TGT table

while [ $a -le 15 ]
do
echo "starting the process for $DAY_ID - $a"
computed_date=$(date -d "${DAY_ID} -${a} days" +%Y-%m-%d)

echo "computed_date is ${computed_date} -- / 
starting hdfs process to remove TGT date for ${computed_date}"

TGT_hdfs_path="${TGT_DIR}/${SRC_TABLE_NAME}/day_id=${computed_date}"

echo "We are about to move the TGT hdfs location, ${TGT_hdfs_path}"

# move HDFS file location 

hdfs dfs -mv ${TGT_hdfs_path} ${TGT_TRUNC_DATA_PATH}/day_id=${computed_date}

if [ $? -eq 0 ];then
	echo "we have succefully moved the data from above partition to safe path hdfs location"
echo -e "\n"

else
	echo "An error occurred while moving the partitions from TGT HDFS to safe path, /
assuming partition was not available and proceeding with other partitions"

echo -e "\n"

fi
a=`expr $a + 1`
hive -e "ALTER TABLE TGT.my_table DROP PARTITION (DAY_ID='${computed_date}')"
done

# Check incase of any partitions still left on TGT layer
echo "************ STARTING PARTITIONS VERIFICATION ON HDFS FOR TGT TABLE ************"

b=0
while [ $b -le 15 ]
do

computed_date=$(date -d "${DAY_ID} -${b} days" +%Y-%m-%d)
hdfs dfs -test -d ${TGT_DIR}/${SRC_TABLE_NAME}/day_id=${computed_date}
if [ $? -eq 0 ];then
echo "exit from program, still data exist on ${TGT_hdfs_path},\
appears something went wrong with partition move, verify the failure manually"
exit_func 4 revert
fi
b=`expr $b + 1`
done
echo -e "\nEverything appears good!!\n"
echo "************ END - HDFS PARTITIONS VERIFICATION FOR TGT TABLE ************"

echo "Repairing the TGT hive table to ensure all parititons information is upto date"

# repair hive table to refresh partition information

hive -e "msck repair table TGT.my_table;analyze table TGT.my_table partition(day_id) compute statistics"
echo "Count of TGT table after data trunc"
hive -e "select count(*) from TGT.my_table"

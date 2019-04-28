#!/bin/ksh/
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#Description- this script will be used to validate counts between Hive and DB 
# 			  tables
#             
#Usage-
# <script_nm> 
# example: sh hive_db_cnts.sh 
#Modification history-
#
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

# Mysql entries sake-
. <db_properties>.properties
queuename=<Hive_queue_nm>
mysqljarLibPath=<mysql_jar_path>


export hive_tbl_list="<tbl_nm1>|<exp_job_tbl_nm1>,<tbl_nm2>|<exp_job_tbl_nm2>"
export DB_tbl_list="<db_tbl_nm1>,<db_tbl_nm2>"
export hive_db_nm="<hive_db_nm>"
export DB_db_nm="<DB_schema_nm>"


######### Prod defs to be enabled on prod
export beeline_jdbc_string='<hive_server2_conn_dtls>'
export HOST_NM="<DB_host_nm>"
export USER_NM="<DB_USER_NM>"
export hadoopSecurityCredentialPath=<path_encrypted>
export sqoopPasswordAlias="<passwd_alias_nm>"
export lake_bkt="<hive_loca_s3_bkt_nm>"

beeline_func () {
i=0
export hive_query="set hive.execution.engine=tez;"
export hive_freshness_query="set hive.execution.engine=tez;"
export Hive_tbl_cnt=$(echo ${hive_tbl_list} | tr "," "\n" | wc -l)

for j in $(echo ${hive_tbl_list} | tr "," "\n")
	do
	hive_tbl_nm=$(echo ${j} | cut -d"|" -f1)
	jobName=$(echo $j | cut -d"|" -f2)
	my_sql_part_val=$(<mysql_connectivity> "${jobName:-'Dummyvalue'}")
	i=$(( ${i} + 1 ))
	if [ ${i} -eq ${Hive_tbl_cnt} ];
		then
			hive_query=${hive_query}"select concat_ws('~',cast(count(*) as string),'"${hive_tbl_nm}"') from "${hive_db_nm}"."${hive_tbl_nm}" WHERE PART_DATE<='"${my_sql_part_val:-'999912315959'}"';"
			hive_freshness_query=${hive_freshness_query}"select concat_ws('~',cast(max(PART_DATE) as string),'"${hive_tbl_nm}"') from "${hive_db_nm}"."${hive_tbl_nm}";"
		else 
			hive_query=${hive_query}"select concat_ws('~',coalesce(cast(count(*) as string),'NA'),'"${hive_tbl_nm}"') from "${hive_db_nm}"."${hive_tbl_nm}" WHERE PART_DATE<='"${my_sql_part_val:-'999912315959'}"' UNION ALL "
			hive_freshness_query=${hive_freshness_query}"select concat_ws('~',cast(max(PART_DATE) as string),'"${hive_tbl_nm}"') from "${hive_db_nm}"."${hive_tbl_nm}" UNION ALL "
	fi
done
export hive_output=$(beeline -u ${beeline_jdbc_string} --silent --outputformat=csv2 --showHeader=false --hiveconf tez.queue.name=${queuename} --hiveconf hive.execution.engine=tez -e "${hive_query}" | sed "/^$/d" |sed "s/ //g")
export hive_freshness_output=$(beeline -u ${beeline_jdbc_string} --silent --outputformat=csv2 --showHeader=false --hiveconf tez.queue.name=${queuename} --hiveconf hive.execution.engine=tez -e "${hive_freshness_query}" | sed "/^$/d" |sed "s/ //g")
}

DB_func () {
j=0
export DB_query=""
export DB_ERR_query=""
export DB_tbl_cnt=$(echo ${DB_tbl_list} | tr "," "\n" | wc -l)
for DB_tbl_nm in $(echo ${DB_tbl_list} | tr "," "\n")
	do
	export DB_ERR_tbl_nm="ERR_${DB_tbl_nm}"
	j=$(( ${j} + 1 ))
	if [ ${j} -eq ${DB_tbl_cnt} ];
		then
			DB_query=${DB_query}"SELECT '~' || CAST(COUNT(*) AS VARCHAR(100)) || '~' || CAST('"${DB_tbl_nm}"' AS VARCHAR(100)) (TITLE '') FROM "${DB_db_nm}"."${DB_tbl_nm}";"
			DB_ERR_query=${DB_ERR_query}"SELECT '~' || CAST(COUNT(*) AS VARCHAR(100)) || '~' || CAST('"${DB_ERR_tbl_nm}"' AS VARCHAR(100)) (TITLE '') FROM "${DB_db_nm}"."${DB_ERR_tbl_nm}";"
		else	
			DB_query=${DB_query}"SELECT '~' || CAST(COUNT(*) AS VARCHAR(100)) || '~' || CAST('"${DB_tbl_nm}"' AS VARCHAR(100)) (TITLE '') FROM "${DB_db_nm}"."${DB_tbl_nm}" UNION ALL "
			DB_ERR_query=${DB_ERR_query}"SELECT '~' || CAST(COUNT(*) AS VARCHAR(100)) || '~' || CAST('"${DB_ERR_tbl_nm}"' AS VARCHAR(100)) (TITLE '') FROM "${DB_db_nm}"."${DB_ERR_tbl_nm}" UNION ALL "
	fi
done

export DB_output=$(sqoop eval -Dhadoop.security.credential.provider.path=jceks://hdfs${hadoopSecurityCredentialPath}/${sqoopPasswordAlias}.password.jceks --connect jdbc:<database_name>://${HOST_NM}/Database=${DB_db_nm} --connection-manager "<required_dtls>" --username ${USER_NM} --password-alias ${sqoopPasswordAlias} --query "${DB_query}" | grep -e "^|" -e "~" | sed -e "s/|//g" -e "s/ //g" -e "s/\*//g" )
}

beeline_func
DB_func

[[ -f full_mail_content.htm ]] && rm full_mail_content.htm

EMAIL_ADDR="<recipient1@<domain>.com>;<recipient2@<domain>.com"
EMAIL_SBJ="counts check on $(hostname) - "$(date +"%D")

cp mail_start_part.html full_mail_content.htm
sed -i "s/EMAIL_TO_ADDRESS/${EMAIL_ADDR}/g" full_mail_content.htm
sed -i "s#EMAIL_SUBJECT#${EMAIL_SBJ}#g" full_mail_content.htm


for x in $(echo "${hive_output}")
do 
dummy=$(echo $x"~"$(echo "${DB_output}" | grep -iw "L_$(echo $x | cut -d"~" -f2)"))

hive_rec_count=$(echo ${dummy} | cut -d"~" -f1)
hive_tname=$(echo ${dummy} | cut -d"~" -f2)
DB_rec_cnt=$(echo ${dummy} | cut -d"~" -f4)
DB_tname=$(echo ${dummy} | cut -d"~" -f5)
DB_ERR_rec_cnt=""
DB_ERR_tname=""

diff_val=$(( ${DB_rec_cnt} - ${hive_rec_count} ))

echo "<tr style='height:15.0pt'>
  
  <td width=214 nowrap valign=bottom style='width:160.4pt;border-top:none;
  border-left:none;border-bottom:solid windowtext 1.0pt;border-right:solid windowtext 1.0pt;
  padding:0in 5.4pt 0in 5.4pt;height:15.0pt'>
  <p ><span class=SpellE><span style='color:black'>${hive_tname}</span></span><span
  style='color:black'></span></p>
  </td>
  <td width=59 nowrap valign=bottom style='width:37.6pt;border:solid windowtext 1.0pt;
  border-top:none;padding:0in 5.4pt 0in 5.4pt;height:15.0pt'>
  <p  align=right style='text-align:right'><span
  style='color:black'>${hive_rec_count}</span></p>
  </td>
  <td width=67 nowrap valign=bottom style='width:44.85pt;border-top:none;
  border-left:none;border-bottom:solid windowtext 1.0pt;border-right:solid windowtext 1.0pt;
  padding:0in 5.4pt 0in 5.4pt;height:15.0pt'>
  <p  align=right style='text-align:right'><span
  style='color:black'>${DB_rec_cnt}</span></p>
  </td>
  <!--
  <td width=67 nowrap valign=bottom style='width:44.85pt;border-top:none;
  border-left:none;border-bottom:solid windowtext 1.0pt;border-right:solid windowtext 1.0pt;
  padding:0in 5.4pt 0in 5.4pt;height:15.0pt'>
  <p  align=right style='text-align:right'><span
  style='color:black'>${DB_ERR_rec_cnt}</span></p>
  </td>
  -->
  
  <td width=128 nowrap valign=bottom style='width:96.0pt;border-top:none;
  border-left:none;border-bottom:solid windowtext 1.0pt;border-right:solid windowtext 1.0pt;
  padding:0in 5.4pt 0in 5.4pt;height:15.0pt'>
  <p  align=right style='text-align:right'><span
  style='color:black'>${diff_val}</span></p>
  </td>
  <td width=213 nowrap valign=bottom style='width:80.0pt;border-top:none;
  border-left:none;border-bottom:solid windowtext 1.0pt;border-right:solid windowtext 1.0pt;
  padding:0in 5.4pt 0in 5.4pt;height:15.0pt'>
  <p ><span style='color:black'>&nbsp;</span></p>
  </td>
 </tr>" >> full_mail_content.htm 
done

echo "</table></div></body>" >> full_mail_content.htm

# hive freshness check

echo "
<p style='text-align: Left;' align='center'><strong><span style='color: black;'>Hive freshness check-</span></strong></p>
<table border='1' width='200px' style='border-collapse:collapse;'>

<tr>
<td style='width: 200px; background: #8CE4FC; height: 3px;border=1;style=border-collapse:collapse;width=200px;cellpadding=0;cellspacing=0' valign='bottom' nowrap='nowrap'>
<p style='text-align: left;' align='center'><strong><span style='color: black;'>MAX_Partition_date </span></strong></p>
</td>
<td style='width: 200px; background: #8CE4FC; height: 3px;border=1;style=border-collapse:collapse;width=200px;cellpadding=0;cellspacing=0' valign='bottom' nowrap='nowrap'>
<p style='text-align: left;' align='center'><strong><span style='color: black;'>Table_nm </span></strong></p>
</td>
</tr>" >> full_mail_content.htm



for x in $(echo "${hive_freshness_output}")
do
mx_date=$(echo $x | cut -d"~" -f1)
fresh_tbl_nm=$(echo $x | cut -d"~" -f2)

echo "
<tr> <td style='width: 200px; background: #8CFCDD; height: 3px;border=1;style=border-collapse:collapse;width=200px;cellpadding=0;cellspacing=0' valign='bottom' wrap='soft'>
<p style='text-align: left;' align='center'>${mx_date} </p></td>

<td style='width: 200px; background: #8CFCDD; height: 3px;border=1;style=border-collapse:collapse;width=200px;cellpadding=0;cellspacing=0' valign='bottom' nowrap='nowrap'>
<p style='text-align: left;' align='center'>${fresh_tbl_nm} </p></td>
</tr>" >> full_mail_content.htm
done
echo "</table>" >> full_mail_content.htm

# new changes
# err file counts
dummy1=$(( $(hdfs dfs -ls s3a://${lake_bkt}/<loc_path>=$(date -d '-1 day' +"%Y%m%d") | wc -l) - 1 ))

[[ ${dummy1} -gt 0 ]] && err_file_cnt=${dummy1} || err_file_cnt=0

dummy2=$(( $(hdfs dfs -ls s3a://${lake_bkt}/<loc_path>=$(date -d '-1 day' +"%Y%m%d") | wc -l) - 1 ))

[[ ${dummy2} -gt 0 ]] && err_file_cnt2=${dummy2} || err_file_cnt2=0

echo "
<p style='text-align: Left;' align='center'><strong><span style='color: black;'>Today Error file counts from load_date -</span></strong></p>

<p style='text-align: Left;' align='center'><strong><span style='color: Brown;'> ${err_file_cnt} </p>

<p style='text-align: Left;' align='center'><strong><span style='color: black;'>Today Error file counts from load_dt -</span></strong></p>

<p style='text-align: Left;' align='center'><strong><span style='color: Brown;'> ${err_file_cnt2} </p>

<p style='text-align: Left;' align='center'><strong><span style='color: black;'>Reprocess Log data - </span></strong></p>
" >> full_mail_content.htm
# err file cnt end

# start of reprocess captures

reproc_qry="set hive.execution.engine=tez;select concat_ws('~',eventtype,cast(cnt as string)) from (select eventtype,count(*) as cnt from <log_table_hive> where substr(etl_load_dt_tm,1,10) = '$(date +"%Y-%m-%d")' group by eventtype )a;"


hive_output_reproc=$(beeline -u ${beeline_jdbc_string} --silent --outputformat=csv2 --showHeader=false --hiveconf tez.queue.name=${queuename} --hiveconf hive.execution.engine=tez -e "${reproc_qry}" | sed "/^$/d" |sed "s/ //g")



echo "
<table border='1' width='200px' style='border-collapse:collapse;'>

<tr>
<td style='width: 200px; background: #FF9933; height: 3px;border=1;style=border-collapse:collapse;width=200px;cellpadding=0;cellspacing=0' valign='bottom' nowrap='nowrap'>
<p style='text-align: left;' align='center'><strong><span style='color: black;'>Eventtype </span></strong></p>
</td>
<td style='width: 200px; background: #FF9933; height: 3px;border=1;style=border-collapse:collapse;width=200px;cellpadding=0;cellspacing=0' valign='bottom' nowrap='nowrap'>
<p style='text-align: left;' align='center'><strong><span style='color: black;'>Count </span></strong></p>
</td>
</tr>" >> full_mail_content.htm

for i in $(echo "${hive_output_reproc}")
do

et_reproc=$(echo $i | cut -d"~" -f1)
cnt_reproc=$(echo $i | cut -d"~" -f2)

echo "
<tr> <td style='width: 200px; background: #FCCC8C; height: 3px;border=1;style=border-collapse:collapse;width=200px;cellpadding=0;cellspacing=0' valign='bottom' wrap='soft'>
<p style='text-align: left;' align='center'>${et_reproc} </p></td>

<td style='width: 200px; background: #FCCC8C; height: 3px;border=1;style=border-collapse:collapse;width=200px;cellpadding=0;cellspacing=0' valign='bottom' nowrap='nowrap'>
<p style='text-align: left;' align='center'>${cnt_reproc} </p></td>
</tr>" >> full_mail_content.htm

done

echo " </table> 
<p style='text-align: left;' align='left'>Note: Hive counts are derived based on latest Mysql partition value at time of execution, usually same value will be used for DB export sake</p>
<p style='text-align: center;' align='center'><strong><span style='color: red;'>For assistance please contact <team_nm> team</span></strong></p> </html>" >> full_mail_content.htm

#!/bin/bash

OFFLINE_IP="192.168.213.245"
HIVE_CMD="/usr/local/hadoop/hive-release/bin/hive"
HADOOP_CMD="/usr/local/hadoop/hadoop-release/bin/hadoop"
GET_HIVESYNC_TICKET="/usr/bin/kinit -r24l -k -t /data/home/hivesync/.keytab hivesync; /usr/bin/kinit -R"
GET_HADOOP_TICKET="/usr/bin/kinit -r24l -k -t /home/hadoop/.keytab hadoop; /usr/bin/kinit -R"

if_tbl_exists() {
	if [ $1 = "local" ]; then
		hive -S -e "use bi; desc $2" > /dev/null 2>&1
		if [ $? -eq 0 ]; then
			return 0
		else 
			return 1
		fi
	elif [ $1 = "remote" ]; then
		ssh -p58422 hivesync@${OFFLINE_IP} "${HIVE_CMD} -S -e 'use bi; desc $2' > /dev/null 2>&1 "	
		if [ $? -eq 0 ]; then
			return 0
		else
			return 1
		fi
	else
		echo "wrong arg."
		exit 1
	fi

}

if_part_exists() {
	#echo "hive -S -e \"use bi; desc formatted $1 partition($2);\""
	hive -S -e "use bi; desc formatted $1 partition($2);" > /dev/null
	if [ $? -ne 0 ]; then
                exit 1
        fi

}

chk_tbl_partlevel() {
	str=`hive -S -e "use bi;show create table $1;"`
	numlevel=`echo $str | grep PARTITIONED | sed -r 's/.*PARTITIONED\sBY\s\((.*)\)\sROW.*/\1/' | wc -w`
	if [ "$numlevel" = "" ]; then
		numlevel=0
	elif [ "$numlevel" -gt 2 ]; then
		echo "ERROR: hivesync.sh does not support tables of multi-level partitions"
		exit 1
	fi
}

get_onlinetbl_path() {
	str=`hive -S -e "use bi; desc formatted $1;"`
	if [ $? -ne 0 ]; then
		exit 1
	fi
	echo $str | sed -r 's/.*Location:\s(.*)\sTable\sType.*/\1/'

}

get_offlinetbl_path() {
	str=`ssh -p58422 hivesync@${OFFLINE_IP} "${HIVE_CMD} -S -e \"use bi; desc formatted $1;\""`
	if [ $? -ne 0 ]; then
                exit 1
        fi
	echo $str | sed -r 's/.*Location:\s(.*)\sTable\sType.*/\1/'
}

get_onlinepart_path() {
	str=`hive -S -e "use bi; desc formatted $1 partition($2);"`
	if [ $? -ne 0 ]; then
                exit 1
        fi
	echo $str | sed -r 's/.*Location:\s(.*)\sPartition.*/\1/'
}

get_offlinepart_path() {
	str=`ssh -p58422 hivesync@${OFFLINE_IP} "${HIVE_CMD} -S -e \"use bi; desc formatted $1 partition($2);\""`
	if [ $? -ne 0 ]; then
                exit 1
        fi
	echo $str | sed -r 's/.*Location:\s(.*)\sPartition.*/\1/'
}

get_hdfspath_size() {
	#echo "hadoop fs -dus $1"
	s0=`hadoop fs -dus $1`
	if [ $? -ne 0 ]; then
		exit 1
	fi
	echo $s0 | awk '{print $2}'
}

get_disk_freesize() {
	if [ $1 = "local" ]; then
		s1=`df -h | grep data | awk '{print $4}'`
		unit=${s1:0-1}
        	if [ $unit = "G" ]; then
                	let bytesize=${s1%*G}*1024*1024*1024 && s1=$bytesize
        	elif [ $unit = "M" ]; then
               		let bytesize=${s1%*M}*1024*1024 && s1=$bytesize
        	elif [ $unit = "K" ]; then
                	let bytesize=${s1%*K}*1024 && s1=$bytesize
        	else
               		echo "little free space left."
                	exit 1
        	fi
	elif [ $1 = "remote" ]; then 
		s2=`ssh -p58422 hivesync@${OFFLINE_IP} "/bin/df -h | grep data | awk '{print \\$4}'"`
		unit=${s2:0-1}
        	if [ $unit = "G" ]; then
               		let bytesize=${s2%*G}*1024*1024*1024 && s2=$bytesize
        	elif [ $unit = "M" ]; then
               		let bytesize=${s2%*M}*1024*1024 && s2=$bytesize
        	elif [ $unit = "K" ]; then
                	let bytesize=${s2%*K}*1024 && s2=$bytesize
        	else
                	echo "little free space left."
                	exit 1
        	fi
	else
		echo "wrong arg."	
		exit 1
	fi
}

get_cluster_freesize() {
	size=`ssh -p58422 hadoop@${OFFLINE_IP} "${HADOOP_CMD} dfsadmin -report | head -n 3 | tail -n 1 | sed -r 's/.*\((.*)\sGB\)/\1/'"`
	if [ $? -ne 0 ]; then
		exit 1
	fi
	bytesize=`echo $size*1024*1024*1024|bc`
	s3=`echo ${bytesize%.*}`
}

get_store_format() {
	serde=`hive -S -e "use bi; desc formatted $1" | grep "SerDe Library" | awk '{print $3}'`
	inputformat=`hive -S -e "use bi; desc formatted $1" | grep "InputFormat" | awk '{print $2}'`
	outputformat=`hive -S -e "use bi; desc formatted $1" | grep "OutputFormat" | awk '{print $2}'`
	if [ $serde = "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe" ]; then
		echo "RCFILE"
	elif [ $serde = "org.apache.hadoop.hive.ql.io.orc.OrcSerde" ]; then
		echo "ORC"
	elif [ $serde = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe" ] && [ $inputformat = "org.apache.hadoop.mapred.SequenceFileInputFormat" ]; then
		echo "SEQUENCEFILE"
	elif [ $serde = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe" ] && [ $inputformat = "org.apache.hadoop.mapred.TextInputFormat" ]; then
		echo "TEXTFILE"
	elif [ $serde = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe" ] && [ $inputformat = "com.hadoop.mapred.DeprecatedLzoTextInputFormat" ]; then
		echo "INPUTFORMAT '"$inputformat"' OUTPUTFORMAT '"$outputformat"'"
	fi
	
}

get_tbl_schema() {
	storeformat=`get_store_format $2`
	#echo $storeformat
	if [ $1 = "local" ]; then
		str=`hive -S -e "use bi; show create table $2;"`
		echo ${str%STORED AS*}" STORED AS "$storeformat | sed 's/u0//g'
		#echo ${str%LOCATION*} | sed 's/u0//g'
	elif [ $1 = "remote" ]; then
		str=`ssh -p58422 hivesync@${OFFLINE_IP} "${HIVE_CMD} -S -e 'use bi; show create table $2;'"`
		echo ${str%STORED AS*}" STORED AS "$storeformat | sed 's/u0//g'
		#echo ${str%LOCATION*} | sed 's/u0//g'
	else
		echo "wront arg."
		exit 1
	fi
}

add_single_partition() {
	#echo "ssh -p58422 hivesync@${OFFLINE_IP} \"${getticket}; ${HIVE_CMD} -S -e \"use bi; alter table $1 drop if exists partition($2);alter table $1 add partition($2);\"\""
	ssh -p58422 hivesync@${OFFLINE_IP} "${HIVE_CMD} -S -e \"use bi; alter table $1 drop if exists partition($2);alter table $1 add partition($2);\""
	
}

get_all_partitions() {
	hive -S -e "use bi;show partitions $1" > $1.partitions
	if [ $? -ne 0 ]; then
                exit 1
        fi
        quote="'"
        sed -i "s/=/=$quote/" $1.partitions
        sed -i "s/$/$quote/" $1.partitions
}

get_range_partitions() {
	get_all_partitions $1
	start_linenum=`grep -n $2 $1.partitions | awk -F":" '{print $1}'`
	if [ $start_linenum -gt 1 ]; then
		let delstart_linenum=$start_linenum-1
		sed -i "1,${delstart_linenum}d" $1.partitions
	fi
	end_linenum=`grep -n $3 $1.partitions | awk -F":" '{print $1}'`
	sum_lines=`cat $1.partitions | wc -l`
	if [ $end_linenum -lt $sum_lines ]; then
		let delend_linenum=$end_linenum+1
		sed -i "${delend_linenum},${sum_lines}d" $1.partitions
	fi
}

add_multi_partitions() {
	if [ -e $1.partitions ]; then
		numparts=`cat "$1.partitions" | wc -l`
                let time=$numparts*5
                echo "INFO: starting to add $1 all partitions ... and it will cost about $time seconds or longer."
                for partition in `cat $1.partitions`
                do
                       add_single_partition $1 $partition
                       #echo
                done
                echo "INFO: add partitions done."
	fi
}

cal_sync_time() {
        str=`echo 0.00000036262552849271*$s0 | bc`
        time=${str%.*}
        if [ "$time" = "" ]; then
                time="several"
        fi
}

#get hivesync and hadoop ticket at first
ssh -p58422 hivesync@${OFFLINE_IP} "${GET_HIVESYNC_TICKET}"
ssh -p58422 hadoop@${OFFLINE_IP} "${GET_HADOOP_TICKET}"

echo "INIT: check whether table, partition exists and the table's partition level is one ..."
# check whether there is enough space to sync data
if [ $# -eq 1 ]; then
	numlevel=""
	if_tbl_exists local $1 && chk_tbl_partlevel $1
	if [ $? -eq 0 ]; then
		echo "OK"
		echo "Step 1/4: checking whether there is enough space to sync data ..."
		tblpath=`get_onlinetbl_path $1`
		s0=0
		s0=`get_hdfspath_size $tblpath`
		#s0="330919890"
	elif [ $? -eq 1 ]; then
		echo "$1 does not exist online."
		exit 1
	fi
elif [ $# -eq 2 ]; then
	if_tbl_exists local $1 && if_part_exists $1 $2 && chk_tbl_partlevel $1
	if [ $? -eq 0 ]; then
		echo "OK"
		echo "Step 1/4: checking whether there is enough space to sync data ..."
		s0=0
		partpath=`get_onlinepart_path $1 $2`
		s0=`get_hdfspath_size $partpath`
		#s0="330919890"
	elif [ $? -eq 1 ]; then
		echo "Either $1 does not exist online or $2 does not exist $1 partition level is more than one."
		exit 1
	fi
elif [ $# -eq 3 ]; then
	if_tbl_exists local $1 && if_part_exists $1 $2 && if_part_exists $1 $3 && chk_tbl_partlevel $1
	if [ $? -eq 0 ]; then
		echo "OK"
		echo "Step 1/4: checking whether there is enough space to sync data ..."
		s0=0
		get_range_partitions $1 $2 $3
		for partition in `cat $1.partitions`
		do
			partpath=`get_onlinepart_path $1 $partition`
			let s0=$s0+`get_hdfspath_size $partpath`
		done
		
	elif [ $? -eq 1 ]; then
		echo "Either $1 does not exist online or $2 does not exist or $3 does not exist or $1 partition level is more than one."
		exit 1
	fi
else
	echo "Usage: $0 tablename [start_partition] [end_partition]"
	exit 1
fi
       
if [ $# -eq 1 ]; then
	echo "INFO: $1 space in bytes: $s0"
elif [ $# -eq 2 ]; then
	echo "INFO: $1($2) space in bytes: $s0"
elif [ $# -eq 3 ]; then
	echo "INFO: $1($2, $3) space in bytes: $s0"
fi
s1=0
get_disk_freesize local
echo "INFO: 7.159 /data free space in bytes: "$s1""

s2=0
get_disk_freesize remote
echo "INFO: cosmos01.beta /data free space in bytes: "$s2""

s3=0
get_cluster_freesize
echo "INFO: offline hadoop cluster free space in bytes: "$s3""

if [ $s0 -gt $s1 ]; then
	echo "ERROR: 7.159 /data has not enough space."
	exit 1
fi

if [ $s0 -gt $s2 ]; then
	echo "ERROR: cosmos01.beta /data has not enough space."
	exit 1
fi

if [ $s0 -gt $s3 ]; then
	echo "ERROR: offline hadoop cluster has not enough space."
fi

if [ $# -eq 1 ]; then
	echo "INFO: There is enough space to transfer $1"
elif [ $# -eq 2 ]; then
	echo "INFO: There is enough space to transfer $1($2)"
elif [ $# -eq 3 ]; then
        echo "INFO: There is enough space to transfer $1($2, $3)"
fi

# sync table schema
echo "Step 2/4: checking online and offline table schema ..."
t1=`get_tbl_schema local $1`
echo "INFO: online $1 schema: "$t1""
if_tbl_exists remote $1
if [ $? -eq 0 ]; then
	t2=`get_tbl_schema remote $1`
	echo "INFO: offline $1 schema: "$t2""
	if [ "$t1" = "$t2" ]; then
		echo "INFO: $1 has the same schema between online and offline, no need to recreate the schema."
	else
		echo "INFO: offline $1 schema is not the same as the online one."
		echo "INFO: dropping table $1 ..."
		ssh -p58422 hivesync@${OFFLINE_IP} "${HIVE_CMD} -S -e 'use bi;drop table $1;' > /dev/null 2>&1"
		if [ $? -eq 0 ]; then
			echo "INFO: drop table $1 done."
			echo "INFO: starting to create table $1 as online..."
			ssh -p58422 hivesync@${OFFLINE_IP} "${HIVE_CMD} -S -e \"use bi;$t1\" > /dev/null 2>&1"
			ssh -p58422 hivesync@${OFFLINE_IP} "${HIVE_CMD} -S -e \"use bi;create table foo(name, string)\""
			if [ $? -eq 0 ]; then
				echo "INFO: create table $1 as online done."
			else
				echo "ERROR: create table $1 as online failed."
				exit 1
			fi
		else
			echo "ERROR: drop table $1 failed."
			exit 1
		fi
	fi
else
	echo "INFO: $1 does not exists offline. creating table $1 as online ..."
	ssh -p58422 hivesync@${OFFLINE_IP} "${HIVE_CMD} -S -e \"use bi;$t1\"" 
	if [ $? -eq 0 ]; then
		echo "INFO: creating table $1 as online done."	
	else
		echo "ERROR: creat table $1 as online failed."
		exit 1
	fi
fi

# add partition
if [ "$numlevel" -eq 0 ]; then
	echo "Step 3/4: no partition table, no need to add."
else
        if [ $# -eq 1 ]; then
        	echo "Step 3/4: checking $1 all partitions ..."
		get_all_partitions $1
		add_multi_partitions $1
        elif [ $# -eq 2 ]; then
        	echo "Step 3/4: starting to add $1 partition($2) ..."
        	add_single_partition $1 $2
        	if [ $? -eq 0 ]; then
        		echo "INFO: $1 add partition $2 done."
        	else
        		echo "ERROR: $1 add partition $2 failed."
        		exit 1
        	fi
	elif [ $# -eq 3 ]; then
		echo "Step 3/4: starting to add $1 partition($2, $3) ..."
		get_range_partitions $1 $2 $3
		add_multi_partitions $1
        fi
fi

# sync data
echo "Step 4/4: sync data from online to offline"
if [ $# -eq 1 ]; then
	oltblpath=`get_offlinetbl_path $1`
	datastamp=`date +%Y%m%d%H%m%S`
	time=0
	cal_sync_time
	echo "INFO: starting to sync $1 data from online to offline ... and it will take about $time seconds or longger."
	echo "hadoop fs -get $tblpath /data/$1_$datastamp"
	hadoop fs -get $tblpath /data/$1_$datastamp
	if [ $? -ne 0 ]; then
                exit 1
        fi
	echo "scp -P58422 -q -r /data/$1_$datastamp hivesync@${OFFLINE_IP}:/data"
	scp -P58422 -q -r /data/$1_$datastamp hivesync@${OFFLINE_IP}:/data
	echo "ssh -p58422 hivesync@${OFFLINE_IP} \"${HADOOP_CMD} fs -rmr $oltblpath/*\""
	ssh -p58422 hivesync@${OFFLINE_IP} "${HADOOP_CMD}  fs -rmr $oltblpath/*"
	if [ $? -ne 0 ]; then
                exit 1
        fi
	echo "ssh -p58422 hivesync@${OFFLINE_IP} \"${HADOOP_CMD} fs -put /data/$1_$datastamp/* $oltblpath\""
	ssh -p58422 hivesync@${OFFLINE_IP} "${HADOOP_CMD}  fs -put /data/$1_$datastamp/* $oltblpath"
	if [ $? -ne 0 ]; then
                exit 1
        fi
	echo "INFO: sync $1 data from online to offline done."
	rm -rf /data/$1_$datastamp
	ssh -p58422 hivesync@${OFFLINE_IP} "rm -rf /data/$1_$datastamp"
	echo "INFO: clear tmp files done."
elif [ $# -eq 2 ]; then
	time=0
	cal_sync_time
	echo "INFO: starting to sync $1($2) data from online to offline ... and it will take about $time seconds or longger."

	olpartpath=`get_offlinepart_path $1 $2`
	datastamp=`date +%Y%m%d%H%m%S`
	echo "hadoop fs -get $partpath /data/$1_$datastamp"
	hadoop fs -get $partpath /data/$1_$datastamp
	if [ $? -ne 0 ]; then
		exit 1
	fi
	echo "scp -P58422 -q -r /data/$1_$datastamp hivesync@${OFFLINE_IP}:/data"
	scp -P58422 -q -r /data/$1_$datastamp hivesync@${OFFLINE_IP}:/data
	if [ $? -ne 0 ]; then
                exit 1
        fi
	echo "ssh -P58422 hivesync@${OFFLINE_IP} \"${HADOOP_CMD} fs -rm $olpartpath/*\""
	ssh -p58422 hivesync@${OFFLINE_IP} "${HADOOP_CMD}  fs -rm $olpartpath/*"
	if [ $? -ne 0 ]; then
                exit 1
        fi
	echo "ssh -p58422 hivesync@${OFFLINE_IP} \"${HADOOP_CMD} fs -put /data/$1_$datastamp/* $olpartpath\""
	ssh -p58422 hivesync@${OFFLINE_IP} "${HADOOP_CMD}  fs -put /data/$1_$datastamp/* $olpartpath"
	if [ $? -ne 0 ]; then
                exit 1
        fi
	echo "INFO: sync $1($2) data from online to offline done."

	rm -f $1.partitions
	rm -rf /data/$1_$datastamp
        ssh -p58422 hivesync@${OFFLINE_IP} "rm -rf /data/$1_$datastamp"
	echo "INFO: clear tmp files done."
elif [ $# -eq 3 ]; then
	datastamp=`date +%Y%m%d%H%m%S`
	for partition in `cat $1.partitions`	
	do
		partpath=`get_onlinepart_path $1 $partition`		
		olpartpath=`get_offlinepart_path $1 $partition`
		quote="'"
		partdir=`echo $partition | sed "s/$quote//g"`
		echo "hadoop fs -get $partpath /data/$1_$datastamp/$partdir"
	        hadoop fs -get $partpath /data/$1_$datastamp/$partdir
        	if [ $? -ne 0 ]; then
        	        exit 1
        	fi
        	echo "scp -P58422 -q -r /data/$1_$datastamp/$partdir hivesync@${OFFLINE_IP}:/data/$1_$datastamp"
		ssh -p58422 hivesync@${OFFLINE_IP} "mkdir -p /data/$1_$datastamp"
        	scp -P58422 -q -r /data/$1_$datastamp/$partdir hivesync@${OFFLINE_IP}:/data/$1_$datastamp/$partdir
        	if [ $? -ne 0 ]; then
        	        exit 1
        	fi
        	echo "ssh -P58422 hivesync@${OFFLINE_IP} \"${HADOOP_CMD} fs -rm $olpartpath/*\""
        	ssh -p58422 hivesync@${OFFLINE_IP} "${HADOOP_CMD}  fs -rm $olpartpath/*"
        	if [ $? -ne 0 ]; then
        	        exit 1
        	fi
        	echo "ssh -p58422 hivesync@${OFFLINE_IP} \"${HADOOP_CMD} fs -put /data/$1_$datastamp/$partdir/* $olpartpath\""
        	ssh -p58422 hivesync@${OFFLINE_IP} "${HADOOP_CMD}  fs -put /data/$1_$datastamp/$partdir/* $olpartpath"
        	if [ $? -ne 0 ]; then
        	        exit 1
        	fi
	done
	echo "INFO: sync $1($2, $3) data from online to offline done."
	
	rm -f $1.partitions
	rm -rf /data/$1_$datastamp
	ssh -p58422 hivesync@${OFFLINE_IP} "rm -rf /data/$1_$datastamp"
	echo "INFO: clear tmp files done."
fi

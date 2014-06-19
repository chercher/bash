#!/bin/bash

OFFLINE_IP="192.168.213.245"
HIVE_CMD="/usr/local/hadoop/hive-release/bin/hive"
HADOOP_CMD="/usr/local/hadoop/hadoop-release/bin/hadoop"

iftblexists() {
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

ifpartexists() {
	#echo "hive -S -e \"use bi; desc formatted $1 partition($2);\""
	hive -S -e "use bi; desc formatted $1 partition($2);" > /dev/null
	if [ $? -ne 0 ]; then
                exit 1
        fi

}

chktblpartlevel() {
	str=`hive -S -e "use bi;show create table $1;"`
	numlevel=`echo $str | grep PARTITIONED | sed -r 's/.*PARTITIONED\sBY\s\((.*)\)\sROW.*/\1/' | wc -w`
	if [ "$numlevel" = "" ]; then
		numlevel=0
	elif [ "$numlevel" -gt 2 ]; then
		echo "ERROR: hivesync.sh does not support tables of multi-level partitions"
		exit 1
	fi
}

gettblpath() {
	str=`hive -S -e "use bi; desc formatted $1;"`
	if [ $? -ne 0 ]; then
		exit 1
	fi
	echo $str | sed -r 's/.*Location:\s(.*)\sTable\sType.*/\1/'

}

getoltblpath() {
	str=`ssh -p58422 hivesync@${OFFLINE_IP} "${HIVE_CMD} -S -e \"use bi; desc formatted $1;\""`
	if [ $? -ne 0 ]; then
                exit 1
        fi
	echo $str | sed -r 's/.*Location:\s(.*)\sTable\sType.*/\1/'
}

getpartpath() {
	str=`hive -S -e "use bi; desc formatted $1 partition($2);"`
	if [ $? -ne 0 ]; then
                exit 1
        fi
	echo $str | sed -r 's/.*Location:\s(.*)\sPartition.*/\1/'
}

getolpartpath() {
	str=`ssh -p58422 hivesync@${OFFLINE_IP} "${HIVE_CMD} -S -e \"use bi; desc formatted $1 partition($2);\""`
	if [ $? -ne 0 ]; then
                exit 1
        fi
	echo $str | sed -r 's/.*Location:\s(.*)\sPartition.*/\1/'
}

gethdfspathsize() {
	#echo "hadoop fs -dus $1"
	s0=`hadoop fs -dus $1`
	if [ $? -ne 0 ]; then
		exit 1
	fi
	s0=`echo $s0 | awk '{print $2}'`
}

getlocalfreesize() {
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

getclusterfreesize() {
	size=`ssh -p58422 hadoop@${OFFLINE_IP} "${HADOOP_CMD} dfsadmin -report | head -n 3 | tail -n 1 | sed -r 's/.*\((.*)\sGB\)/\1/'"`
	bytesize=`echo $size*1024*1024*1024|bc`
	s3=`echo ${bytesize%.*}`
}

getfileformat() {
	serde=`hive -S -e "use bi; desc formatted $1" | grep "SerDe Library" | awk '{print $3}'`
	inputformat=`hive -S -e "use bi; desc formatted $1" | grep "InputFormat" | awk '{print $2}'`
	if [ $serde = "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe" ]; then
		echo "RCFILE"
	elif [ $serde = "org.apache.hadoop.hive.ql.io.orc.OrcSerde" ]; then
		echo "ORC"
	elif [ $serde = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe" ] && [ $inputformat = "org.apache.hadoop.mapred.SequenceFileInputFormat" ]; then
		echo "SEQUENCEFILE"
	elif [ $serde = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe" ] && [ $inputformat = "org.apache.hadoop.mapred.TextInputFormat" ]; then
		echo "TEXTFILE"
	fi
	
}

gettblschema() {
	fileformat=`getfileformat $2`
	if [ $1 = "local" ]; then
		str=`hive -S -e "use bi; show create table $2;"`
		echo ${str%STORED AS*}" STORED AS "$fileformat | sed 's/u0//g'
	elif [ $1 = "remote" ]; then
		str=`ssh -p58422 hivesync@${OFFLINE_IP} "${HIVE_CMD} -S -e 'use bi; show create table $2;'"`
		echo ${str%STORED AS*}" STORED AS "$fileformat | sed 's/u0//g'
	else
		echo "wront arg."
		exit 1
	fi
}

addpartition() {
	ssh -p58422 hivesync@${OFFLINE_IP} "${HIVE_CMD} -S -e \"use bi; alter table $1 drop if exists partition($2);alter table $1 add partition($2);\""
	
}

# check whether there is enough space to sync data
if [ $# -eq 1 ]; then
	echo "INIT: check whether table, partition exists and the table's partition level is one ..."
	numlevel=""
	iftblexists local $1 && chktblpartlevel $1
	if [ $? -eq 0 ]; then
		echo "OK"
		echo "Step 1/4: checking whether there is enough space to sync data ..."
		tblpath=`gettblpath $1`
		s0=""
		gethdfspathsize $tblpath
		#s0="330919890"
	elif [ $? -eq 1 ]; then
		echo "$1 does not exist online."
		exit 1
	fi
elif [ $# -eq 2 ]; then
	echo "INIT: checking whether table, partition exists and the table's partition level is one..."
	iftblexists local $1 && ifpartexists $1 $2 && chktblpartlevel $1
	if [ $? -eq 0 ]; then
		echo "OK"
		echo "Step 1/4: checking whether there is enough space to sync data ..."
		partpath=`getpartpath $1 $2`
		s0=""
		gethdfspathsize $partpath
		#s0="330919890"
	elif [ $? -eq 1 ]; then
		echo "$1 does not exist online."
		exit 1
	fi
else
	echo "Usage: $0 tablename [partitionname]"
	exit 1
fi
       
if [ $# -eq 1 ]; then
	echo "INFO: $1 space in bytes: "$s0""
elif [ $# -eq 2 ]; then
	echo "INFO: $1($2) space in bytes: "$s0""
fi
s1=""
getlocalfreesize local
echo "INFO: 7.159 /data free space in bytes: "$s1""

s2=""
getlocalfreesize remote
echo "INFO: cosmos01.beta /data free space in bytes: "$s2""

s3=""
getclusterfreesize
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
fi

# sync table schema
echo "Step 2/4: checking online and offline table schema ..."
t1=`gettblschema local $1`
echo "INFO: online $1 schema: "$t1""
iftblexists remote $1
if [ $? -eq 0 ]; then
	t2=`gettblschema remote $1`
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
        	echo "Step 3/4: checking $1 partitions ..."
        	hive -S -e "use bi;show partitions $1" > $1.partitions
        	if [ -e $1.partitions ]; then
        		numparts=`cat "$1.partitions" | wc -l`
        		let time=$numparts*5
        		echo "INFO: starting to add $1 all partitions ... and it will cost about $time seconds or longer."
        		quote="'"
        		sed -i "s/=/=$quote/" $1.partitions
        		sed -i "s/$/$quote/" $1.partitions
        		for partition in `cat $1.partitions`
        		do
        			addpartition $1 $partition	
        			#echo
        		done
        		echo "INFO: add partitions done."
        			
        	fi
        elif [ $# -eq 2 ]; then
        	echo "Step 3/4: starting to add $1 partition($2) ..."
        	addpartition $1 $2
        	if [ $? -eq 0 ]; then
        		echo "INFO: $1 add partition $2 done."
        	else
        		echo "ERROR: $1 add partition $2 failed."
        		exit 1
        	fi
        fi
fi

#sync data
echo "Step 4/4: sync data from online to offline"
if [ $# -eq 1 ]; then
	oltblpath=`getoltblpath $1`
	datastamp=`date +%Y%m%d%H%m%S`
	str=`echo 0.00000036262552849271*$s0 | bc`
	time=${str%.*}	
	echo "INFO: starting to sync $1 data from online to offline ... and it will take about $time seconds or longger."
	echo "hadoop fs -get $tblpath /data/$1_$datastamp"
	hadoop fs -get $tblpath /data/$1_$datastamp
	if [ $? -ne 0 ]; then
                exit 1
        fi
	echo "scp -P58422 -q -r /data/$1_$datastamp hivesync@${OFFLINE_IP}:/data"
	scp -P58422 -q -r /data/$1_$datastamp hivesync@${OFFLINE_IP}:/data
	ssh -p58422 hivesync@${OFFLINE_IP} "/usr/bin/kinit -r24l -k -t /data/home/hivesync/.keytab hivesync; /usr/bin/kinit -R"
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
	olpartpath=`getolpartpath $1 $2`
	datastamp=`date +%Y%m%d%H%m%S`
	str=`echo 0.00000036262552849271*$s0 | bc`
        time=${str%.*}
	if [ "$time" = "" ]; then
		time="several"
	fi
	echo "INFO: starting to sync $1($2) data from online to offline ... and it will take about $time seconds or longger."
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
	rm -rf /data/$1_$datastamp
        ssh -p58422 hivesync@${OFFLINE_IP} "rm -rf /data/$1_$datastamp"
	echo "INFO: clear tmp files done."
fi



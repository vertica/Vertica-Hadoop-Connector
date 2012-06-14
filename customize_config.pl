############################
# Usage: binary
# 	path to config directory
# 	JAVA_HOME
# 	port        # specify ports starting from this port.
#	test directory
#	number of nodes
#	path to vertica JDBC jar
############################

$configdir = $ARGV[0];
$java = $ARGV[1];
$port = $ARGV[2];
$testdir = $ARGV[3];
$numslaves = $ARGV[4];
$verticajar = $ARGV[5];
$hadoop_vertica_jars = $ARGV[6];
$hname = `hostname`;
chomp $hname;

open (HADOOP_ENV, "<$configdir/hadoop-env.sh") or die "Could not open $configdir/hadoop-env.sh";
@henv = <HADOOP_ENV>;
close(HADOOP_ENV);

for ($count = 0; $count <= $#henv; $count++) {
	if ($henv[$count] =~ /export JAVA_HOME/) {
		$henv[$count] = "export JAVA_HOME=$java\n";
	}
	if ($henv[$count] =~ /export HADOOP_LOG_DIR/) {
		$henv[$count] = "export HADOOP_LOG_DIR=$testdir/logs\n";
	}
	if ($henv[$count] =~ /export HADOOP_PID_DIR/) {
		$henv[$count] = "export HADOOP_PID_DIR=$testdir/\n";
	}
	if ($henv[$count] =~ /export HADOOP_CLASSPATH/) {
		$henv[$count] = "export HADOOP_CLASSPATH=$verticajar:$hadoop_vertica_jars\n";
	}
	if ($henv[$count] =~ /export HADOOP_TASKTRACKER_OPTS/) {
		$henv[$count] = "export HADOOP_TASKTRACKET_OPT='-classpath $hadoop_vertica_jars'\n";
	}
}

open (HADOOP_ENV, ">$configdir/hadoop-env.sh") or die "Could not open $configdir/hadoop-env.sh";
print HADOOP_ENV join("", @henv);
close(HADOOP_ENV);

open (LOGPROPS, "<$configdir/log4j.properties") or die "Could not open $configdir/log4j.properties";
@logprops = <LOGPROPS>;
close(LOGPROPS);

@newlogprops = ();
for ($count = 0; $count <= $#logprops; $count++) {
	if ($logprops[$count] =~ /^hadoop.log.dir/) {
		push(@newlogprops, "hadoop.log.dir=$testdir" . "/logs" . "\n");
	} elsif ($logprops[$count] =~ /Custom Logging levels/) {
		push(@newlogprops, $logprops[$count]);
		push(@newlogprops, "log4j.logger.com.vertica.hadoop=INFO" . "\n");
	} else {
		push(@newlogprops, $logprops[$count]);
	}
}

open (LOGPROPS, ">$configdir/log4j.properties") or die "Could not open $configdir/log4j.properties";
print LOGPROPS join("", @newlogprops);
close(LOGPROPS);

open (CORESITE, ">$configdir/core-site.xml") or die "Could not open $configdir/core-site.xml";
print CORESITE <<CORESITEEOF;
<configuration>
	<property>
		<name>fs.default.name</name>
			<value>hdfs://$hname:$port</value>
	</property>
</configuration>
CORESITEEOF

close(CORESITE);
$port++;

open (HDFSSITE, ">$configdir/hdfs-site.xml") or die "Could not open $configdir/hdfs-site.xml";
print HDFSSITE <<HDFSSITEEOF;
<configuration>
	<property>
		<name>hadoop.tmp.dir</name>
		<value>$testdir</value>
	</property>
	<property>
		<name>dfs.replication</name>
		<value>1</value>
	</property>
</configuration>
HDFSSITEEOF

close(HDFSSITE);

open (MAPREDSITE, ">$configdir/mapred-site.xml") or die "Could not open $configdir/mapred-site.xml";
print MAPREDSITE <<MAPREDSITEEOF;
<configuration>
	<property>
		<name>mapred.job.tracker</name>
			<value>$hname:$port</value>
	</property>
</configuration>
MAPREDSITEEOF

close(MAPREDSITE);

open (SLAVES, ">$configdir/slaves") or die "Could not open $configdir/slaves";
for ($count = 0; $count < $numslaves; $count++) {
	print SLAVES "$hname\n";
}
close(SLAVES);

open (MASTER, ">$configdir/masters") or die "Could not open $configdir/masters";
print MASTER "$hname\n";
close(MASTER);

exit(0);

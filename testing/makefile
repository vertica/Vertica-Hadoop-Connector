#+++++
#
#  Description:
#
#	Build the Vertica Hadoop System: 
#-----
#	$Source$
###
# Load the standard make definitions
###
ifndef VERTICA_SOURCE
  export VERTICA_SOURCE := $(shell perl \
    -e '$$_="$(PWD)"; do {print,exit if -d "$$_/make"} while s:/[^/]*$$::')
endif
ifndef JDBC_SOURCE
  export JDBC_SOURCE := $(VERTICA_SOURCE)/client/JDBC/vjdbc
endif
SOURCE := $(VERTICA_SOURCE)
include	$(VERTICA_SOURCE)/make/include.mk

ifndef HADOOP_SOURCE
   $(echo "HADOOP_SOURCE is required. It should point to a checkout of SVNROOT/binary/Hadoop")
endif

export HADOOP_DISTRO := $(HADOOP_SOURCE)/$(DISTRO_NAME)

include $(HADOOP_SOURCE)/$(DISTRO_NAME)/parameters.mk
include $(HADOOP_SOURCE)/variables.mk

export HADOOP_HOME_WARN_SUPPRESS := 1
export JAR_DIR := $(TARGET)/hadoop

export HADOOP_START_CMD := hadoop-start
export HADOOP_STOP_CMD :=  hadoop-stop

ifdef USE_YARN
  export HADOOP_START_CMD := yarn-start
  export HADOOP_STOP_CMD :=  yarn-stop
endif

pig_version = $(shell JAVA_HOME=$(JDK16) HADOOP_HOME=$(HADOOP_HOME) $(PIG_HOME)/bin/pig -version 2>/dev/null) 

# pg_regress parameters
export DRIVER  := $(TARGET)/pig_driver.sh
PIG_LIB :=  $(PIG_HOME)/lib

export TMPDIR := $(PG_TESTOUT)/tmp
export DIFF := BSdiff

$(TMPDIR):
	$(MKDIR) -p $(TMPDIR)

$(DRIVER): test_driver.sh $(TMPDIR)
	@sed -e 's,@libdir@,$(PIG_LIB),g' \
	    -e 's,@pkglibdir@,$(PIG_LIB),g' \
	    -e 's/@VERSION@/$(PIG_VERSION)/g' \
	    -e 's,@GMAKE@,$(MAKE),g' \
	    -e 's/@GCC@/$(GCC)/g' \
	  $< >$@
	@chmod a+x $@
export VERTICA_CLUSTER_INFORMATION := $(shell \
	if [ -f $(PG_TESTOUT)/config/hadoop.vertica.info ]; then \
	  cat $(PG_TESTOUT)/config/hadoop.vertica.info; fi)

run-example : 
	JAVA_HOME=${JDK16} ${HADOOP_HOME}/bin/hadoop jar ${JAR_DIR}/hadoop-vertica-example.jar com.vertica.hadoop.VerticaExample ${VERTICA_CLUSTER_INFORMATION} -conf $(HADOOP_CONF_DIR)/core-site.xml
	JAVA_HOME=${JDK16} ${HADOOP_HOME}/bin/hadoop fs -mkdir testing/datasource 
	JAVA_HOME=${JDK16} ${HADOOP_HOME}/bin/hadoop fs -copyFromLocal $(VERTICA_SOURCE)/tools/hadoop/testing/data/datasource testing/datasource 
	JAVA_HOME=${JDK16} ${HADOOP_HOME}/bin/hadoop fs -copyFromLocal $(VERTICA_SOURCE)/tools/hadoop/testing/data/ints.dat testing/ints.dat 
	JAVA_HOME=${JDK16} ${HADOOP_HOME}/bin/hadoop fs -copyFromLocal $(VERTICA_SOURCE)/tools/hadoop/testing/data/chars.dat testing/chars.dat 
	JAVA_HOME=${JDK16} ${HADOOP_HOME}/bin/hadoop fs -copyFromLocal $(VERTICA_SOURCE)/tools/hadoop/testing/data/chars2.dat testing/chars2.dat 
	JAVA_HOME=${JDK16} ${HADOOP_HOME}/bin/hadoop fs -copyFromLocal $(VERTICA_SOURCE)/tools/hadoop/testing/data/chars3.dat testing/chars3.dat 
	JAVA_HOME=${JDK16} ${HADOOP_HOME}/bin/hadoop jar ${JAR_DIR}/hadoop-vertica-example.jar com.vertica.hadoop.hdfs2vertica ${VERTICA_CLUSTER_INFORMATION} -conf $(HADOOP_CONF_DIR)/core-site.xml "testing/datasource" "streaming" "|" "intcol,floatcol,charcol,varcharcol,boolcol,datecol,timestampcol,timestampTzcol,timecol,timeTzcol,varbincol,bincol,numcol,intervalcol"
	JAVA_HOME=${JDK16} ${HADOOP_HOME}/bin/hadoop jar ${JAR_DIR}/hadoop-vertica-example.jar com.vertica.hadoop.hdfs2vertica ${VERTICA_CLUSTER_INFORMATION} -conf $(HADOOP_CONF_DIR)/core-site.xml "testing/datasource" "streaming_with_id" "|" "intcol,floatcol,charcol,varcharcol,boolcol,datecol,timestampcol,timestampTzcol,timecol,timeTzcol,varbincol,bincol,numcol,intervalcol"
	JAVA_HOME=${JDK16} ${HADOOP_HOME}/bin/hadoop jar ${JAR_DIR}/hadoop-vertica-example.jar com.vertica.hadoop.hdfs2vertica ${VERTICA_CLUSTER_INFORMATION} -conf $(HADOOP_CONF_DIR)/core-site.xml "testing/ints.dat" "ints" "|" "a,b"
	JAVA_HOME=${JDK16} ${HADOOP_HOME}/bin/hadoop jar ${JAR_DIR}/hadoop-vertica-example.jar com.vertica.hadoop.hdfs2vertica ${VERTICA_CLUSTER_INFORMATION} -conf $(HADOOP_CONF_DIR)/core-site.xml "testing/ints.dat" "ints" "|" "a,b"
	JAVA_HOME=${JDK16} ${HADOOP_HOME}/bin/hadoop jar ${JAR_DIR}/hadoop-vertica-example.jar com.vertica.hadoop.hdfs2vertica ${VERTICA_CLUSTER_INFORMATION} -conf $(HADOOP_CONF_DIR)/core-site.xml "testing/ints.dat" "ints_different" "|" "a,b"
	JAVA_HOME=${JDK16} ${HADOOP_HOME}/bin/hadoop jar ${JAR_DIR}/hadoop-vertica-example.jar com.vertica.hadoop.hdfs2vertica ${VERTICA_CLUSTER_INFORMATION} -conf $(HADOOP_CONF_DIR)/core-site.xml "testing/ints.dat" "ints_more" "|" "a,b"
	JAVA_HOME=${JDK16} ${HADOOP_HOME}/bin/hadoop jar ${JAR_DIR}/hadoop-vertica-example.jar com.vertica.hadoop.hdfs2vertica ${VERTICA_CLUSTER_INFORMATION} -conf $(HADOOP_CONF_DIR)/core-site.xml "testing/ints.dat" "ints_less" "|" "a,b"
	JAVA_HOME=${JDK16} ${HADOOP_HOME}/bin/hadoop jar ${JAR_DIR}/hadoop-vertica-example.jar com.vertica.hadoop.hdfs2vertica ${VERTICA_CLUSTER_INFORMATION} -conf $(HADOOP_CONF_DIR)/core-site.xml "testing/chars.dat" "char_only" "|" "a,b"
	JAVA_HOME=${JDK16} ${HADOOP_HOME}/bin/hadoop jar ${JAR_DIR}/hadoop-vertica-example.jar com.vertica.hadoop.hdfs2vertica ${VERTICA_CLUSTER_INFORMATION} -conf $(HADOOP_CONF_DIR)/core-site.xml "testing/chars.dat" "char_10_only" "|" "a,b"
	JAVA_HOME=${JDK16} ${HADOOP_HOME}/bin/hadoop jar ${JAR_DIR}/hadoop-vertica-example.jar com.vertica.hadoop.hdfs2vertica ${VERTICA_CLUSTER_INFORMATION} -conf $(HADOOP_CONF_DIR)/core-site.xml "testing/chars.dat" "varchar_only" "|" "a,b"
	JAVA_HOME=${JDK16} ${HADOOP_HOME}/bin/hadoop jar ${JAR_DIR}/hadoop-vertica-example.jar com.vertica.hadoop.hdfs2vertica ${VERTICA_CLUSTER_INFORMATION} -conf $(HADOOP_CONF_DIR)/core-site.xml "testing/chars.dat,testing/chars2.dat,testing/chars3.dat" "varchar_only" "|" "a,b"
	JAVA_HOME=${JDK16} ${HADOOP_HOME}/bin/hadoop fs -rmr testing 

streaming :
	JAVA_HOME=${JDK16} STREAM_JAR=${STREAM_JAR} HADOOP_HOME_WARN_SUPPRESS=1 HADOOP_HOME=${HADOOP_HOME} VERTICA_CLUSTER_INFORMATION="$(VERTICA_CLUSTER_INFORMATION)" JAR_DIR=$(JAR_DIR) $(VERTICA_SOURCE)/tools/hadoop/testing/streaming/vertica2vertica
	JAVA_HOME=${JDK16} STREAM_JAR=${STREAM_JAR} HADOOP_HOME_WARN_SUPPRESS=1 HADOOP_HOME=${HADOOP_HOME} VERTICA_CLUSTER_INFORMATION="$(VERTICA_CLUSTER_INFORMATION)" JAR_DIR=$(JAR_DIR) $(VERTICA_SOURCE)/tools/hadoop/testing/streaming/hdfs2vertica "streaming" "|"
	JAVA_HOME=${JDK16} STREAM_JAR=${STREAM_JAR} HADOOP_HOME_WARN_SUPPRESS=1 HADOOP_HOME=${HADOOP_HOME} VERTICA_CLUSTER_INFORMATION="$(VERTICA_CLUSTER_INFORMATION)" JAR_DIR=$(JAR_DIR) $(VERTICA_SOURCE)/tools/hadoop/testing/streaming/hdfs2vertica "streaming_with_id" "|"
	JAVA_HOME=${JDK16} STREAM_JAR=${STREAM_JAR} HADOOP_HOME_WARN_SUPPRESS=1 HADOOP_HOME=${HADOOP_HOME} VERTICA_CLUSTER_INFORMATION="$(VERTICA_CLUSTER_INFORMATION)" JAR_DIR=$(JAR_DIR) $(VERTICA_SOURCE)/tools/hadoop/testing/streaming/hdfs2vertica "streaming" "\|"

export NAMENODE := http://$(shell hostname):$(WEBHDFS_PORT)/webhdfs/v1/user/$(shell whoami)
hdfs_test:
	JAVA_HOME=${JDK16} ${HADOOP_HOME}/bin/hadoop fs -mkdir hdfslib 
	JAVA_HOME=${JDK16} ${HADOOP_HOME}/bin/hadoop fs -copyFromLocal $(VERTICA_SOURCE)/tools/hadoop/testing/hdfslib/* hdfslib
	JAVA_HOME=${JDK16} ${HADOOP_HOME}/bin/hadoop fs -chmod 600 hdfslib/glob_test/region_nopermissions.tbl 
	(cd $(VERTICA_SOURCE); echo $(HDFS_LIB_PATH);VTEST_NOSTOP=yes $(MAKE) -C SQLTest hdfs_test)
	JAVA_HOME=${JDK16} ${HADOOP_HOME}/bin/hadoop fs -rmr hdfslib 

connector_test: $(DRIVER)
	@$(MAKE) HADOOP_HOME_WARN_SUPPRESS=1 -C pig-connector checkin_test_all
ifeq ($(findstring 0.7,$(pig_version)),0.7)
	@echo "Squeal tests not enabled yet"
	#@$(MAKE) -C testing/squeal $@
endif
	@(cd $(VERTICA_SOURCE); $(MAKE) -C SQLTest hadoop_setup.sql)
	@$(MAKE) run-example streaming
	@(cd $(VERTICA_SOURCE); $(MAKE) -C SQLTest hadoop_example_check.sql hadoop_teardown.sql)
	
.PHONY: streaming hadoop-example hadoop-connector pig-connector squeal hadoop-src checkin_test checkin_test_all test_checkresults 

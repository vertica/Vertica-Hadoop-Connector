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

# location for test output
export PG_TESTOUT ?= $(TARGET)/Test/

export HADOOP_HOME_WARN_SUPPRESS := 1
export JAR_DIR := $(TARGET)/hadoop
export HADOOP_CLASSPATH := $(JDBC_SOURCE)/../jars/vertica.jar:$(JAR_DIR)/hadoop-vertica.jar

export HADOOP_START_CMD := hadoop-start
export HADOOP_STOP_CMD :=  hadoop-stop

ifdef USE_YARN
  export HADOOP_START_CMD := yarn-start
  export HADOOP_STOP_CMD :=  yarn-stop
endif

copy_docs: doc
	ln -s $(JAR_DIR)/doc $(DOC_LOC)

hadoop-src: 
	$(MAKE) -C $(HADOOP_SOURCE) hadoop-src

jar: hadoop-src 
	make -C src $@

doc:
	make -C src $@

clean:
	make -C src $@
	@rm -f $(JAR_DIR)/hadoop-pig-vertica.zip

pkg-src: 
	rsync -az src $(JAR_DIR) --exclude=\*.svn* --exclude=makefile
	rsync -az testing/data $(JAR_DIR) --exclude=\*.svn*
	rsync -az README.txt $(JAR_DIR) --exclude=\*.svn*

package: jar doc pkg-src
	cd $(JAR_DIR); zip -rq hadoop-pig-vertica.zip hadoop-vertica.jar pig-vertica.jar hadoop-vertica-example.jar doc/ src/ data/ README.txt

$(HADOOP_TESTBASE):
	$(MKDIR) $(HADOOP_TESTBASE)

# This target calls SummarizeTestFailures that looks for difs and core files
test_checkresults:
	@perl $(MAKE_DIR)/bin/SummarizeTestFailures.pl $(TARGET) $(PG_TESTOUT)

start_clusters: 
	@$(MAKE) -C $(HADOOP_SOURCE) $(HADOOP_START_CMD) save_hadoop_info
	@$(MAKE) -C $(VERTICA_SOURCE)/SQLTest test_setup_4node save_vertica_info

stop_clusters: 
	@$(MAKE) -C $(HADOOP_SOURCE) $(HADOOP_STOP_CMD)
	@$(MAKE) -C $(VERTICA_SOURCE)/SQLTest cluster_shutdown

# pg_regress parameters
export DRIVER  := $(TARGET)/pig_driver.sh
PIG_LIB :=  $(PIG_HOME)/lib

export TMPDIR := $(PG_TESTOUT)/tmp
export DIFF := BSdiff

$(TMPDIR):
	$(MKDIR) -p $(TMPDIR)

connector_test_only:
	@$(MAKE) start_clusters
	@$(MAKE) -C testing connector_test
	@$(MAKE) stop_clusters
	@(cd $(VERTICA_SOURCE); $(MAKE) -C SQLTest test_checkresults)

hdfs_test_only:
	@$(MAKE) start_clusters
	$(MAKE) -C testing hdfs_test
	@$(MAKE) stop_clusters
	@(cd $(VERTICA_SOURCE); $(MAKE) -C SQLTest test_checkresults)

checkin_test:
	@$(MAKE) start_clusters
	@$(MAKE) -C testing hdfs_test
	@$(MAKE) -C testing connector_test
	@$(MAKE) stop_clusters
	@(cd $(VERTICA_SOURCE); $(MAKE) -C SQLTest test_checkresults)

cdh3_test_all:
	DISTRO_NAME=CDH2 $(MAKE) connector_test_only

hw_test_all:
	DISTRO_NAME=HortonWorks $(MAKE) connector_test_only

apache_test_all:
	DISTRO_NAME=Apache_10 $(MAKE) clean jar
	DISTRO_NAME=Apache_10 $(MAKE) checkin_test

cdh4_test_all:
	DISTRO_NAME=CDH4 $(MAKE) clean jar
	DISTRO_NAME=CDH4 $(MAKE) checkin_test

yarn_test_all:
	DISTRO_NAME=CDH4-Yarn $(MAKE) clean jar
	DISTRO_NAME=CDH4-Yarn USE_YARN=true $(MAKE) hdfs_test_only

test_all_distro: yarn_test_all apache_test_all cdh3_test_all hw_test_all cdh4_test_all

.PHONY: streaming hadoop-example hadoop-connector pig-connector squeal hadoop-src checkin_test checkin_test_all test_checkresults 

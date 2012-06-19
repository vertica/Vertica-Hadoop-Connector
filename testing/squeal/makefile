#+++++
#
#  Description:
#
#	The SQLTest directory contains various Vertica tests, generally
#	intended to be run by make from this directory, e.g.,
#	'make checkin_test' or 'make setup_test qa_test'
#
#	Tests can also be run directly from the vertica/makefile 
#
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
  export JDBC_SOURCE := $(shell perl \
    -e '$$_="$(PWD)"; do {print,exit if -d "$$_/make"} while s:/[^/]*$$::')
endif
SOURCE := $(VERTICA_SOURCE)
include	$(VERTICA_SOURCE)/make/include.mk
include	$(HADOOP_DISTRO)/parameters.mk


# location for test output
export PG_TESTOUT ?= $(TARGET)/Test/

include $(PG_TESTOUT)/config/hadoop.env

#ADD YOUR TEST SUITES HERE
checkin_test:
	$(MAKE) squeal_smoke

checkin_test_all:
	$(MAKE) VTEST_NOSTOP=yes checkin_test


####################THIS SECTION IS FOR THE BRAVE####################################
export PWD := $(SOURCE)/tools/hadoop/testing/squeal

export PIG_OPTS := $(shell \
	if [ -f $(PG_TESTOUT)/config/hadoop.vertica.info ]; then \
	  cat $(PG_TESTOUT)/config/hadoop.vertica.info; fi)

export PIG_CLASSPATH := $(CLASSPATH):$(JAR_DIR)/hadoop-vertica.jar:$(JAR_DIR)/pig-vertica.jar:$(JAR_DIR)/squeal.jar:$(SOURCE)/client/JDBC/jars/vertica.jar
export SQUEAL_ROOT_LOGGER := DEBUG
export JAVA_HOME := $(JDK16)
#export CLIENT_OPTIONS := -b -d WARN -p script=$(PWD)/scripts -M
export CLIENT_OPTIONS := -b -d WARN -param_file $(PG_TESTOUT)/config/pig.vertica.info -p script=$(PWD)/scripts $(VERBOSE)
export SQUEAL_BIN :=  $(PWD)/squeal

###
# Customize the makefile
###

export DRIVER  := $(TARGET)/pig_driver.sh

DRIVER_OPTIONS := 
ifdef ignore
    DRIVER_OPTIONS += --ignore='$(ignore)'
endif

# Rule to run pig scripts individually
vpath %.pig scripts/*
%.pig : 
	@echo $(SOURCE)
ifdef VTEST_NOSTOP
	@ -( $(DRIVER) $(DRIVER_OPTIONS) --client=$(SQUEAL_BIN) --outputdir=$(PG_TESTOUT)/$* $* )
else
	@ $(DRIVER) $(DRIVER_OPTIONS) --client=$(SQUEAL_BIN) --outputdir=$(PG_TESTOUT)/$* $*
endif

# if VTEST_NOSTOP is defined, ignore PG_REGRESS errors (aka don't stop make)
squeal_%: 
ifdef VTEST_NOSTOP
	@ -( $(DRIVER) --client=$(SQUEAL_BIN) --inputdir=$(PWD) --outputdir=$(PG_TESTOUT)/$@ --schedule=$@.in $(DRIVER_OPTIONS) )
else
	@ $(DRIVER) --client=$(SQUEAL_BIN) --inputdir=$(PWD) --outputdir=$(PG_TESTOUT)/$@ --schedule=$@.in $(DRIVER_OPTIONS)
endif

.PHONY: 

###
# Package tree files to export
# Format is <destination>=<src><,options...>, where destination may be
#   a filename or a directory, and options may be a comma-separated list
###
PACKAGE_FILES := 

###
# Customize the makefile
###
include	$(MAKE_DIR)/support.mk

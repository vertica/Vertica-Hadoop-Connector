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
  export JDBC_SOURCE := "$(VERTICA_SOURCE)/client/JDBC/vjdbc"
endif
SOURCE := $(VERTICA_SOURCE)
include	$(VERTICA_SOURCE)/make/include.mk
include	$(HADOOP_DISTRO)/parameters.mk

# location for test output
export PG_TESTOUT ?= $(TARGET)/Test/

include $(PG_TESTOUT)/config/hadoop.env

#ADD YOUR TEST SUITES HERE
checkin_test:
	$(MAKE) pigtest_smoke

checkin_test_all:
	$(MAKE) VTEST_NOSTOP=yes checkin_test


####################THIS SECTION IS FOR THE BRAVE####################################
export PWD := $(shell pwd)
export PIG_CLASSPATH := $(JDBC_SOURCE)/../jars/:$(HADOOP_HOME)/lib:$(JAR_DIR)/
export PIG_ROOT_LOGGER := WARN
export JAVA_HOME := $(JDK16)
export CLIENT_OPTIONS := -b -d WARN -param_file $(PG_TESTOUT)/config/pig.vertica.info -p script=$(PWD)/scripts -M -param pvjar=$(JAR_DIR)/pig-vertica.jar -param jdbcjar=$(JDBC_SOURCE)/../jars/vertica.jar
PIG_BIN :=  $(PIG_HOME)/bin/pig

###
# Customize the makefile
###
export DISTRO_NAME := $(shell basename $(HADOOP_DISTRO))
export DRIVER  := $(TARGET)/pig_driver.sh

DRIVER_OPTIONS := 
ifdef ignore
    DRIVER_OPTIONS += --ignore='$(ignore)'
endif

# Rule to run pig scripts individually
vpath %.pig scripts/*
%.pig : 
	@echo $(VERTICA_SOURCE)
	@(cd $(VERTICA_SOURCE); $(MAKE) -C SQLTest hadoop_setup.sql)
ifdef VTEST_NOSTOP
	@ -( $(DRIVER) $(DRIVER_OPTIONS) --client=$(PIG_BIN) --outputdir=$(PG_TESTOUT)/$* $* )
else
	@ $(DRIVER) $(DRIVER_OPTIONS) --client=$(PIG_BIN) --outputdir=$(PG_TESTOUT)/$* $*
endif
	@(cd $(VERTICA_SOURCE); $(MAKE) -C SQLTest hadoop_teardown.sql)

# if VTEST_NOSTOP is defined, ignore PG_REGRESS errors (aka don't stop make)
pig_version = $(shell $(PIG_BIN) -version)
pigtest_%: 
	@echo "$(pig_version)"
	@(cd $(VERTICA_SOURCE); $(MAKE) -C $(VERTICA_SOURCE)/SQLTest hadoop_setup.sql)
ifdef VTEST_NOSTOP
	@ -($(DRIVER) --client=$(PIG_BIN) --inputdir=$(PWD) --outputdir=$(PG_TESTOUT)/$@ --schedule=$@.in $(DRIVER_OPTIONS) )
else
	@ ($(DRIVER) --client=$(PIG_BIN) --inputdir=$(PWD) --outputdir=$(PG_TESTOUT)/$@ --schedule=$@.in $(DRIVER_OPTIONS) )
endif
	@(cd $(VERTICA_SOURCE); $(MAKE) -C $(VERTICA_SOURCE)/SQLTest hadoop_teardown.sql)

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

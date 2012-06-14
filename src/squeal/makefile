#+++++
#
#  Description:
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

export BUILD_ARGS := -lib ${PIG_HOME}/lib -Dversion=$(VERSION) -Dhadoop.dir=${HADOOP_HOME} -Dpig.jar=${PIG_HOME}/${PIG_JAR} -Dbuild.dir=${TARGET}/squeal -Ddist=${JAR_DIR}  -Dvertica.jar=$(JDBC_SOURCE)/client/JDBC/jars/vertica.jar -Dhadoop-connector.jar=$(JAR_DIR)/hadoop-vertica.jar -Dpig-connector.jar=$(JAR_DIR)/pig-vertica.jar

squeal :
	JAVA_HOME=${JDK16} ant ${BUILD_ARGS} 
clean:
	JAVA_HOME=${JDK16} ant -Dbuild.dir=${HADOOP_DIR}/squeal -Ddist=${JAR_DIR} clean 
SRCS := squeal

###
# Define the standard way to build libraries
###
include	$(MAKE_DIR)/build.mk

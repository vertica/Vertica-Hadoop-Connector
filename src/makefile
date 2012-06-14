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

include $(HADOOP_SOURCE)/$(DISTRO_NAME)/parameters.mk
include $(HADOOP_SOURCE)/variables.mk

JDBC:
	@echo "JDBC in $(JDBC_SOURCE) ..."
	(cd $(JDBC_SOURCE); ant clean jar)

export BUILD_ARGS := -lib ${HADOOP_HOME}/${HADOOP_JAR} -lib ${PIG_HOME}/lib -Dversion=$(VERSION) -Dhadoop.dir=${HADOOP_HOME} -Dbuild.dir=${TARGET} -Ddist=${JAR_DIR} -Djunit.output.dir=$(PG_TESTOUT)/hadoop_junit -Dpig.jar=${PIG_HOME}/${PIG_JAR} -Dpig.version="$(pig_version)" -Dvertica.jar=$(JDBC_SOURCE)/../jars/vertica.jar -Dhadoop-connector.jar=$(JAR_DIR)/hadoop-vertica.jar -Dpig-connector.jar=$(JAR_DIR)/pig-vertica.jar -Ddoc.dir=$(JAR_DIR)/doc/

jar: JDBC
	JAVA_HOME=${JDK16} ant $(BUILD_ARGS) jar

doc:
	JAVA_HOME=${JDK16} ant $(BUILD_ARGS) doc

clean:
	JAVA_HOME=${JDK16} ant $(BUILD_ARGS) $@

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

export BUILD_ARGS := -lib ${HADOOP_HOME}/${HADOOP_JAR} -lib ${PIG_HOME}/lib -Dversion=$(VERSION) -Dhadoop.dir=${HADOOP_HOME} -Dbuild.dir=${TARGET} -Ddist=${JAR_DIR} -Djunit.output.dir=$(PG_TESTOUT)/hadoop_junit -Dpig.jar=${PIG_HOME}/pig-$(pig_version)-withouthadoop.jar -Dpig.version="$(pig_version)" -Dvertica.jar=$(VERTICA_JAR) -Dhadoop-connector.jar=$(JAR_DIR)/hadoop-vertica.jar -Dpig-connector.jar=$(JAR_DIR)/pig-vertica.jar -Ddoc.dir=$(JAR_DIR)/doc/

jar: 
	JAVA_HOME=${JDK16} ant $(BUILD_ARGS) jar

doc:
	JAVA_HOME=${JDK16} ant $(BUILD_ARGS) doc

clean:
	JAVA_HOME=${JDK16} ant $(BUILD_ARGS) $@

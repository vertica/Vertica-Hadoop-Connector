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
export BUILD_ARGS := -lib ${HADOOP_HOME}/ -lib ${PIG_HOME}/lib -Dhadoop.dir=${HADOOP_HOME} -Dbuild.dir=${BUILDDIR} -Ddist=${JAR_DIR} -Dpig.jar=${PIG_HOME}/pig-$(pig_version)-withouthadoop.jar -Dpig.version="$(pig_version)" -Dvertica.jar=$(VERTICA_JAR) -Dhadoop-connector.jar=$(JAR_DIR)/hadoop-vertica.jar -Dpig-connector.jar=$(JAR_DIR)/pig-vertica.jar -Ddoc.dir=$(JAR_DIR)/doc/

jar: 
	JAVA_HOME=${JDK16} ant $(BUILD_ARGS) jar

doc:
	JAVA_HOME=${JDK16} ant $(BUILD_ARGS) doc

clean:
	JAVA_HOME=${JDK16} ant $(BUILD_ARGS) $@

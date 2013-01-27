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
export BUILD_ARGS :=-Dhadoop.dir=${HADOOP_HOME} -Dpig.jar=${PIG_JAR}

jar: 
	JAVA_HOME=${JDK16} ant $(BUILD_ARGS) jar

doc:
	JAVA_HOME=${JDK16} ant $(BUILD_ARGS) doc

clean:
	JAVA_HOME=${JDK16} ant $(BUILD_ARGS) $@

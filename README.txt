****************************
* Vertica Analytic Database
*
* Apache Hadoop & Pig Connector
*
* Copyright 2012 Vertica Systems, an HP company 2012
****************************

This directory contains jars, examples & source code for 
Apache Hadoop & Pig connectors to the Vertica Analytic Database.

IMPORTANT: If you wish to contribute anything to this repository, in
order for us to accept your pull request you MUST sign and send a copy
of the appropriate Contributor License Agreement to Vertica
(contribs@vertica.com):

license/PersonalCLA.pdf: If you are contributing for yourself
license/CorporateCLA.pdf: If you are contributing on behalf of your company

Directory structure:
  ./hadoop-connector  Hadoop to Vertica Connector
  ./pig-connector     Pig to Vertica Connector
  ./hadoop-example    Example for using the Hadoop Connector
  ./squeal            Squeal - a pig to sql translator
  ./sql               SQL scripts to setup tables for hadoop-example
  ./unit_test         unit tests for the connector - dev purpose
  ./license           licenses

Compile:
   Set the following environment variables: 
   HADOOP_HOME - Directory containing the hadoop installation.
   PIG_HOME   - Directory containing Apache Pig installation.
   PIG_JAR-     Name of pig jar , usually located in $PIG_HOME
   BUILDDIR   - Temporary directory for storing build files.
   pig_version- Pig version.
   VERTICA_JAR- Path to Vertica JDBC jar.
   JAR_DIR    - Directory where the new jar files & docs should be stored.

   Run - make jar

HOW TO RUN hadoop-vertica-example.jar:
   If everything goes well, the above 'make jar' produces
   hadoop-vertica-example.jar in the $JAR_DIR. Before running the example, make
   sure you create allTypes table using
   
   vsql -f sql/hadoop_example_setup.sql 
   
   which is required for the example to run.

   Now to run the example, the command is

   hadoop jar hadoop-vertica-example.jar com.vertica.hadoop.VerticaExample
   -libjars /usr/lib/hadoop/lib/hadoop-vertica.jar,/usr/lib/hadoop/lib/vertica-jdk5.jar
   -Dmapred.vertica.hostnames=host1,host2,host3,host4
   -Dmapred.vertica.port=5433 -Dmapred.vertica.username=userfoo
   -Dmapred.vertica.database=TESTDB


   where , 
  
   -libjars points to absolute path of hadoop-vertica.jar & Vertica JDBC jar
   -Dmapred.vertica.hostnames - comma separated hosts in the Vertica cluster
   -Dmapred.vertica.port  - Vertica port
   -Dmapred.vertica.username  - Vertica username
   -Dmapred.vertica.database - Vertica database name

   Make sure the vertica user has right permissions on the hdfs file system.

   After running the example, make sure you drop the table by using,

   vsql -f sql/hadoop_example_cleanup.sql

 
TODO: 
    Use Maven to download dependencies.
	Examples for pig connector.
	Examples for Hadoop streaming.

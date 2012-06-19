****************************
* Vertica Analytic Database
*
* Apache Hadoop & Pig Connector
*
* Copyright 2012 Vertica Systems, an HP company 2012
****************************

This directory contains jars, examples, doc & source code for 
Apache Hadoop & Pig connectors to the Vertica Analytic Database.

IMPORTANT: If you wish to contribute anything to this repository, in
order for us to accept your pull request you MUST sign and send a copy
of the appropriate Contributor License Agreement to Vertica
(contribs@vertica.com):

license/PersonalCLA.pdf: If you are contributing for yourself
license/CorporateCLA.pdf: If you are contributing on behalf of your company

Directory structure:
  ./hadoop-vertica    Hadoop to Vertica Connector
  ./pig-vertica       Pig to Vertica Connector
  ./hadoop-vertica-example Example for using the Hadoop Connector
  ./squeal            Squeal - a pig to sql translator.

Compile:
   Set the following environment variables: 
   HADOOP_HOME - Directory containing the hadoop installation.
   PIG_HOME   - Directory containing Apache Pig installation.
   BUILDDIR   - Temporary directory for storing build files.
   pig_version- Pig version.
   VERTICA_JAR- Path to Vertica JDBC jar.
   JAR_DIR    - Directory where the new jar files & docs should be stored.

   Run - make jar
TODO: 
    Use Maven to download dependencies.
	helpful commands for running examples.
	Examples for pig connector.
	Examples for Hadoop streaming.

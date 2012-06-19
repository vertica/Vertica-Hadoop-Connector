package com.vertica.squeal;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.StringWriter;

import java.sql.Connection;
import java.sql.DriverManager;


import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.text.ParseException;

import jline.ConsoleReader;
import jline.ConsoleReaderInputStream;
import jline.History;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import org.apache.pig.*;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.ExecType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.*;
import org.apache.pig.impl.util.JarManager;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.PropertiesUtil;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.tools.pigstats.PigStatsUtil;
import org.apache.pig.tools.cmdline.CmdLineParser;
import org.apache.pig.tools.grunt.Grunt;
import org.apache.pig.tools.grunt.GruntParser;
import org.apache.pig.impl.util.LogUtils;
import org.apache.pig.tools.timer.PerformanceTimerFactory;
import org.apache.pig.tools.parameters.ParameterSubstitutionPreprocessor;

/**
 * Main class for Pig engine.
 */
@InterfaceAudience.LimitedPrivate({"Oozie"})
@InterfaceStability.Stable
public class Squeal {
   
    private final static Log log = LogFactory.getLog(Squeal.class);
    
  	/** Class name for Vertica JDBC Driver */
	public static final String VERTICA_DRIVER_CLASS = "com.vertica.jdbc.Driver";
	public static final String VERTICA_DRIVER_CLASS_41 = "com.vertica.Driver";

    private static final String LOG4J_CONF = "log4jconf";
    private static final String BRIEF = "brief";
    private static final String DEBUG = "debug";
    private static final String JAR = "jar";
    private static final String VERBOSE = "verbose";
    private static final String JDBCURL = "jdbc.url";
    private static final String USERNAME = "jdbc.username";
    private static final String PASSWORD = "jdbc.password";
    
    private enum ExecMode {STRING, FILE, SHELL, UNKNOWN}    
                
/**
 * The Main-Class for the Pig Jar that will provide a shell and setup a classpath appropriate
 * for executing Jar files.  Warning, this method calls System.exit().
 * 
 * @param args
 *            -jar can be used to add additional jar files (colon separated). - will start a
 *            shell. -e will execute the rest of the command line as if it was input to the
 *            shell.
 * @throws IOException
 */
public static void main(String args[])
{
    int rc = 1;
    Properties properties = new Properties();
    PropertiesUtil.loadPropertiesFromFile(properties);
    
    boolean verbose = false;
    boolean gruntCalled = false;
    String logFileName = null;
    boolean userSpecifiedLog = false;
    boolean runPig = false;

    try {
        BufferedReader pin = null;
        boolean debug = false;
        boolean dryrun = false;
        ArrayList<String> params = new ArrayList<String>();
        ArrayList<String> paramFiles = new ArrayList<String>();
        HashSet<String> optimizerRules = new HashSet<String>();

        CmdLineParser opts = new CmdLineParser(args);
        opts.registerOpt('4', "log4jconf", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('b', "brief", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        opts.registerOpt('c', "cluster", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('d', "debug", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('e', "execute", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        opts.registerOpt('f', "file", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('h', "help", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        opts.registerOpt('i', "version", CmdLineParser.ValueExpected.OPTIONAL);
        opts.registerOpt('j', "jar", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('l', "logfile", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('m', "param_file", CmdLineParser.ValueExpected.OPTIONAL);
        opts.registerOpt('p', "param", CmdLineParser.ValueExpected.OPTIONAL);
        opts.registerOpt('P', "pig", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        opts.registerOpt('r', "dryrun", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        opts.registerOpt('t', "optimizer_off", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('v', "verbose", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        opts.registerOpt('w', "warning", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        opts.registerOpt('x', "exectype", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('F', "stop_on_failure", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        opts.registerOpt('M', "no_multiquery", CmdLineParser.ValueExpected.NOT_ACCEPTED);

        ExecMode mode = ExecMode.UNKNOWN;
        String file = null;
        ExecType execType = ExecType.MAPREDUCE ;
        String execTypeString = properties.getProperty("exectype");
        if(execTypeString!=null && execTypeString.length()>0){
            execType = PigServer.parseExecType(execTypeString);
        }
        String cluster = "local";
        String clusterConfigured = properties.getProperty("cluster");
        if(clusterConfigured != null && clusterConfigured.length() > 0){
            cluster = clusterConfigured;
        }
        
        //by default warning aggregation is on
        properties.setProperty("aggregate.warning", ""+true);

        //by default multiquery optimization is on
        properties.setProperty("opt.multiquery", ""+true);

        //by default we keep going on error on the backend
        properties.setProperty("stop.on.failure", ""+false);
        
        // set up client side system properties in UDF context
        UDFContext.getUDFContext().setClientSystemProps();

        char opt;
        while ((opt = opts.getNextOpt()) != CmdLineParser.EndOfOpts) {
            switch (opt) {
            case '4':
                String log4jconf = opts.getValStr();
                if(log4jconf != null){
                    properties.setProperty(LOG4J_CONF, log4jconf);
                }
                break;

            case 'b':
                properties.setProperty(BRIEF, "true");
                break;

            case 'c': 
                // Needed away to specify the cluster to run the MR job on
                // Bug 831708 - fixed
                String clusterParameter = opts.getValStr();
                if (clusterParameter != null && clusterParameter.length() > 0) {
                    cluster = clusterParameter;
                }
                break;

            case 'd':
                String logLevel = opts.getValStr();
                if (logLevel != null) {
                    properties.setProperty(DEBUG, logLevel);
                }
                debug = true;
                break;
                
            case 'e': 
                mode = ExecMode.STRING;
                break;

            case 'f':
                mode = ExecMode.FILE;
                file = opts.getValStr();
                break;

            case 'F':
                properties.setProperty("stop.on.failure", ""+true);
                break;

            case 'h':
                usage();
                rc = 0;
                return;

            case 'i':
            	System.out.println(getVersionString());
                rc = 0;
            	return;

            case 'j': 
                String jarsString = opts.getValStr();
                if(jarsString != null){
                    properties.setProperty(JAR, jarsString);
                }
                break;

            case 'l':
                //call to method that validates the path to the log file 
                //and sets up the file to store the client side log file                
                String logFileParameter = opts.getValStr();
                if (logFileParameter != null && logFileParameter.length() > 0) {
                    logFileName = validateLogFile(logFileParameter, null);
                } else {
                    logFileName = validateLogFile(logFileName, null);
                }
                userSpecifiedLog = true;
                properties.setProperty("pig.logfile", (logFileName == null? "": logFileName));
                break;

            case 'm':
                paramFiles.add(opts.getValStr());
                break;

            case 'M':
                // turns off multiquery optimization
		log.info("Cannot turn off multiquery for Squeal");
                //properties.setProperty("opt.multiquery",""+false);
                break;
		
            case 'p': 
                String val = opts.getValStr();
                params.add(opts.getValStr());
                break;
                            
	    case 'P':
		runPig = true;
		break;
                            
            case 'r': 
                // currently only used for parameter substitution
                // will be extended in the future
                dryrun = true;
                break;

            case 't':
            	optimizerRules.add(opts.getValStr());
                break;
		
            case 'v':
                properties.setProperty(VERBOSE, ""+true);
                verbose = true;
                break;

            case 'w':
                properties.setProperty("aggregate.warning", ""+false);
                break;

            case 'x':
                try {
                    execType = PigServer.parseExecType(opts.getValStr());
                    } catch (IOException e) {
                        throw new RuntimeException("ERROR: Unrecognized exectype.", e);
                    }
                break;
            default: {
                Character cc = Character.valueOf(opt);
                throw new AssertionError("Unhandled option " + cc.toString());
                     }
            }
        }

        // create the context with the parameter
        PigContext pigContext = new PigContext(execType, properties);

        if(logFileName == null && !userSpecifiedLog) {
	    logFileName = validateLogFile(properties.getProperty("pig.logfile"), null);
	}
        
        if(logFileName != null) {
            log.info("Logging error messages to: " + logFileName);
        }
        
        pigContext.getProperties().setProperty("pig.logfile", (logFileName == null? "": logFileName));
     
        // configure logging
        configureLog4J(properties, pigContext);
        
        
        if(optimizerRules.size() > 0) {
        	pigContext.getProperties().setProperty("pig.optimizer.rules", ObjectSerializer.serialize(optimizerRules));
        }
        
        if (properties.get("udf.import.list")!=null)
            PigContext.initializeImportList((String)properties.get("udf.import.list"));

        LogicalPlanBuilder.classloader = pigContext.createCl(null);

        // construct the parameter substitution preprocessor
        String remainders[] = opts.getRemainingArgs();
		if (file == null) {
			if (remainders == null)
			    throw new RuntimeException("You must specific a script to translate.");

			// They have a pig script they want us to run.
			if (remainders.length > 1) {
	    		throw new RuntimeException("You can only run one pig script "
				       + "at a time from the command line.");

			}
			mode = ExecMode.FILE;
			file = remainders[0];
		}
		BufferedReader in = new BufferedReader(new FileReader(file));

	// run parameter substitution preprocessor first
	String substFile = file + ".substituted";
	pin = runParamPreprocessor(in, params, paramFiles, substFile);
            
	if (!debug) {
	    new File(substFile).deleteOnExit();
	}

	// Set job name based on name of the script
	pigContext.getProperties().setProperty(PigContext.JOB_NAME, 
					       "PigLatin:" +new File(file).getName()
					       );

	log.info("Constructing server");
	PigServer svr = new PigServer(pigContext);

	log.info("Configuring vertica connection");


    Properties sysprops = System.getProperties();
    String hostname = sysprops.getProperty("mapred.vertica.hostnames", "localhost");
    String username = sysprops.getProperty("mapred.vertica.username", "dbadmin");
    String password = sysprops.getProperty("mapred.vertica.password", "");
    String database = sysprops.getProperty("mapred.vertica.database", "");
    String port = sysprops.getProperty("mapred.vertica.port", "");
	
	//InputScript is = new InputScript(pigContext);
	//is.readScript(substFile);


	BufferedReader infile = new BufferedReader(new FileReader(new File(substFile)));
	LOTranslator trans = new LOTranslator();
	Connection conn = null;
	if (!dryrun) {
    	try {
	      Class.forName(VERTICA_DRIVER_CLASS);
    	} catch (ClassNotFoundException e) {
			try {
				Class.forName(VERTICA_DRIVER_CLASS_41);
			} catch (ClassNotFoundException e2) {
				throw new RuntimeException(e);
			}
    	}
	    conn = DriverManager.getConnection("jdbc:vertica://" + hostname + 
					       ":" + port + "/" + database, username, password);
	}
	SquealParser parser = new SquealParser(infile, trans, conn, verbose, dryrun, runPig);

	parser.setParams(svr);
	parser.setInteractive(false);
	while (!parser.isDone()) {
	    parser.parse();
	}

	parser.cleanup();

	return;

        // Per Utkarsh and Chris invocation of jar file via pig depricated.
    } catch (ParseException e) {
	usage();
        rc = 2;
    } catch (NumberFormatException e) {
	usage();
        rc = 2;
    } catch (PigException pe) {
        if(pe.retriable()) {
            rc = 1; 
        } else {
            rc = 2;
        }
	pe.printStackTrace();
    } catch (Throwable e) {
        rc = 2;
	e.printStackTrace();
    } finally {
        // clear temp files
        FileLocalizer.deleteTempFiles();
        PerformanceTimerFactory.getPerfTimerFactory().dumpTimers();
        System.exit(rc);
    }
}

private static int getReturnCodeForStats(int[] stats) {
    if (stats[1] == 0) {
        // no failed jobs
        return 0;
    }
    else {
        if (stats[0] == 0) {
            // no succeeded jobs
            return 2;
        }
        else {
            // some jobs have failed
            return 3;
        }
    }
}
    
//TODO jz: log4j.properties should be used instead
private static void configureLog4J(Properties properties, PigContext pigContext) {
    // TODO Add a file appender for the logs
    // TODO Need to create a property in the properties file for it.
    // sgroschupf, 25Feb2008: this method will be obsolete with PIG-115.
     
    String log4jconf = properties.getProperty(LOG4J_CONF);
    String trueString = "true";
    boolean brief = trueString.equalsIgnoreCase(properties.getProperty(BRIEF));
    Level logLevel = Level.INFO;

    String logLevelString = properties.getProperty(DEBUG);
    if (logLevelString != null){
        logLevel = Level.toLevel(logLevelString, Level.INFO);
    }
    
    Properties props = new Properties();
    FileReader propertyReader = null;
    if (log4jconf != null) {
        try {
            propertyReader = new FileReader(log4jconf);
            props.load(propertyReader);
        }
        catch (IOException e)
        {
            System.err.println("Warn: Cannot open log4j properties file, use default");
        }
        finally
        {
            if (propertyReader != null) try {propertyReader.close();} catch(Exception e) {}
        }
    }
    if (props.size() == 0) {
        props.setProperty("log4j.logger.org.apache.pig", logLevel.toString());
        if((logLevelString = System.getProperty("pig.logfile.level")) == null){
            props.setProperty("log4j.rootLogger", "INFO, PIGCONSOLE");
        }
        else{
            logLevel = Level.toLevel(logLevelString, Level.INFO);
            props.setProperty("log4j.logger.org.apache.pig", logLevel.toString());
            props.setProperty("log4j.rootLogger", "INFO, PIGCONSOLE, F");
            props.setProperty("log4j.appender.F","org.apache.log4j.RollingFileAppender");
            props.setProperty("log4j.appender.F.File",properties.getProperty("pig.logfile"));
            props.setProperty("log4j.appender.F.layout","org.apache.log4j.PatternLayout");
            props.setProperty("log4j.appender.F.layout.ConversionPattern", brief ? "%m%n" : "%d [%t] %-5p %c - %m%n");
        }
        
        props.setProperty("log4j.appender.PIGCONSOLE","org.apache.log4j.ConsoleAppender");    
        props.setProperty("log4j.appender.PIGCONSOLE.target", "System.err");        
        props.setProperty("log4j.appender.PIGCONSOLE.layout","org.apache.log4j.PatternLayout");
        props.setProperty("log4j.appender.PIGCONSOLE.layout.ConversionPattern", brief ? "%m%n" : "%d [%t] %-5p %c - %m%n");
    }

    PropertyConfigurator.configure(props);
    logLevel = Logger.getLogger("org.apache.pig").getLevel();
    Properties backendProps = pigContext.getLog4jProperties();
    backendProps.setProperty("log4j.logger.org.apache.pig.level", logLevel.toString());
    pigContext.setLog4jProperties(backendProps);
    pigContext.setDefaultLogLevel(logLevel);
}

// returns the stream of final pig script to be passed to Grunt
private static BufferedReader runParamPreprocessor(BufferedReader origPigScript, ArrayList<String> params,
                                            ArrayList<String> paramFiles, String scriptFile) 
                                throws org.apache.pig.tools.parameters.ParseException, IOException{
    ParameterSubstitutionPreprocessor psp = new ParameterSubstitutionPreprocessor(50);
    String[] type1 = new String[1];
    String[] type2 = new String[1];

    BufferedWriter fw = new BufferedWriter(new FileWriter(scriptFile));
    psp.genSubstitutedFile (origPigScript, fw, params.size() > 0 ? params.toArray(type1) : null, 
			    paramFiles.size() > 0 ? paramFiles.toArray(type2) : null);
    return new BufferedReader(new FileReader (scriptFile));
}

private static String getVersionString() {
	String findContainingJar = JarManager.findContainingJar(Main.class);
	  try { 
          JarFile jar = new JarFile(findContainingJar); 
          final Manifest manifest = jar.getManifest(); 
          final Map <String,Attributes> attrs = manifest.getEntries(); 
          Attributes attr = attrs.get("org/apache/pig");
          String version = attr.getValue("Implementation-Version");
          String svnRevision = attr.getValue("Svn-Revision");
          String buildTime = attr.getValue("Build-TimeStamp");
          // we use a version string similar to svn 
          //svn, version 1.4.4 (r25188)
          // compiled Sep 23 2007, 22:32:34
          return "Apache Pig version " + version + " (r" + svnRevision + ") \ncompiled "+buildTime;
      } catch (Exception e) { 
          throw new RuntimeException("unable to read pigs manifest file", e); 
      } 
}

/**
 * Print usage string.
 */
public static void usage()
{
	System.out.println("\n"+getVersionString()+"\n");
        System.out.println("USAGE: Squeal [options] file : Translate script to SQL.");
        System.out.println("  options include:");
        System.out.println("    -4, -log4jconf log4j configuration file, overrides log conf");
        System.out.println("    -b, -brief brief logging (no timestamps)");
        System.out.println("    -c, -cluster clustername, kryptonite is default");
        System.out.println("    -d, -debug debug level, INFO is default");
        System.out.println("    -h, -help display this message");
        System.out.println("    -i, -version display version information");
        System.out.println("    -j, -jar jarfile load jarfile"); 
        System.out.println("    -l, -logfile path to client side log file; current working directory is default");
        System.out.println("    -m, -param_file path to the parameter file");
        System.out.println("    -p, -param key value pair of the form param=val");
        System.out.println("    -t, -optimizer_off optimizer rule name, turn optimizer off for this rule; use all to turn all rules off, optimizer is turned on by default");
        System.out.println("    -v, -verbose print all error messages to screen");
        System.out.println("    -w, -warning turn warning on; also turns warning aggregation off");
        System.out.println("    -x, -exectype local|mapreduce, mapreduce is default");

        System.out.println("    -F, -stop_on_failure aborts execution on the first failed job; off by default");
        System.out.println("    -M, -no_multiquery turn multiquery optimization off; Multiquery is on by default");
}

private static String validateLogFile(String logFileName, String scriptName) {
    String strippedDownScriptName = null;
    
    if(scriptName != null) {
        File scriptFile = new File(scriptName);
        if(!scriptFile.isDirectory()) {
            String scriptFileAbsPath;
            try {
                scriptFileAbsPath = scriptFile.getCanonicalPath();
            } catch (IOException ioe) {
                log.warn("Could not compute canonical path to the script file " + ioe.getMessage());
                return null;
            }            
            strippedDownScriptName = getFileFromCanonicalPath(scriptFileAbsPath);
        }
    }
    
    String defaultLogFileName = (strippedDownScriptName == null ? "pig_" : strippedDownScriptName) + new Date().getTime() + ".log";
    File logFile;    
    
    if(logFileName != null) {
        logFile = new File(logFileName);
    
        //Check if the file name is a directory 
        //append the default file name to the file
        if(logFile.isDirectory()) {            
            if(logFile.canWrite()) {
                try {
                    logFileName = logFile.getCanonicalPath() + File.separator + defaultLogFileName;
                } catch (IOException ioe) {
                    log.warn("Could not compute canonical path to the log file " + ioe.getMessage());
                    return null;
                }
                return logFileName;
            } else {
                log.warn("Need write permission in the directory: " + logFileName + " to create log file.");
                return null;
            }
        } else {
            //we have a relative path or an absolute path to the log file
            //check if we can write to the directory where this file is/will be stored
            
            if (logFile.exists()) {
                if(logFile.canWrite()) {
                    try {
                        logFileName = new File(logFileName).getCanonicalPath();
                    } catch (IOException ioe) {
                        log.warn("Could not compute canonical path to the log file " + ioe.getMessage());
                        return null;
                    }
                    return logFileName;
                } else {
                    //do not have write permissions for the log file
                    //bail out with an error message
                    log.warn("Cannot write to file: " + logFileName + ". Need write permission.");
                    return logFileName;
                }
            } else {
                logFile = logFile.getParentFile();
                
                if(logFile != null) {
                    //if the directory is writable we are good to go
                    if(logFile.canWrite()) {
                        try {
                            logFileName = new File(logFileName).getCanonicalPath();
                        } catch (IOException ioe) {
                            log.warn("Could not compute canonical path to the log file " + ioe.getMessage());
                            return null;
                        }
                        return logFileName;
                    } else {
                        log.warn("Need write permission in the directory: " + logFile + " to create log file.");
                        return logFileName;
                    }
                }//end if logFile != null else is the default in fall through                
            }//end else part of logFile.exists()
        }//end else part of logFile.isDirectory()
    }//end if logFileName != null
    
    //file name is null or its in the current working directory 
    //revert to the current working directory
    String currDir = System.getProperty("user.dir");
    logFile = new File(currDir);
    logFileName = currDir + File.separator + (logFileName == null? defaultLogFileName : logFileName);
    if(logFile.canWrite()) {        
        return logFileName;
    }    
    log.warn("Cannot write to log file: " + logFileName);
    return null;
}

private static String getFileFromCanonicalPath(String canonicalPath) {
    return canonicalPath.substring(canonicalPath.lastIndexOf(File.separator));
}

}

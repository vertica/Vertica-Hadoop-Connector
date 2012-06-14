
package com.vertica.hadoop;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.vertica.hadoop.VerticaConfiguration;

/**
 * All tests for Vertica Formatters (com.vertica.hadoop)
 * 
 * 
 */
public final class AllTests {
  private static final Log LOG = LogFactory.getLog(AllTests.class);

  static final String VERTICA_HOSTNAME = "localhost";
  static final String VERTICA_USERNAME = "dbadmin";
  static final String VERTICA_PASSWORD = "";
  static final String VERTICA_DATABASE = "db";

  static String hostname;
  static String username;
  static String password;
  static String database;
  static String port;

  static boolean run_tests = false;
  
  public static String getHostname() {
    return hostname;
  }

  public static String getUsername() {
    return username;
  }

  public static String getPassword() {
    return password;
  }

  public static String getDatabase() {
    return database;
  }

  public static String getPort() {
    return port;
  }
  
  public static boolean isSetup() {
    return run_tests;
  } 

  private AllTests() {
  }

  public static void configure() {
    if (run_tests) {
      return;
    }

    Properties properties = System.getProperties();

    String test_setup = properties.getProperty("vertica.test_setup", "vertica_test.sql");
    hostname = properties.getProperty("mapred.vertica.hostnames", VERTICA_HOSTNAME);
    username = properties.getProperty("mapred.vertica.username", VERTICA_USERNAME);
    password = properties.getProperty("mapred.vertica.password", VERTICA_PASSWORD);
    database = properties.getProperty("mapred.vertica.database", VERTICA_DATABASE);
    port = properties.getProperty("mapred.vertica.port", VERTICA_DATABASE);
	run_tests = true;
	System.out.println(hostname + "," + username + "," + password + "," + database + "," + port);
  }

  public static Test suite() {
    configure();
    TestSuite suite = new TestSuite("Tests for com.hadoop.vertica");

    if (run_tests) {
      suite.addTestSuite(TestVertica.class);
      suite.addTestSuite(TestExample.class);
    }
    return suite;
  }

}

/* Copyright (c) 2005 - 2012 Vertica, an HP company -*- Java -*- */
package com.vertica.squeal;

import java.io.PrintStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.List;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.pig.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;

public class ScriptRunner {

    private final static Log log = LogFactory.getLog(ScriptRunner.class);

    PigServer svr;
    Connection conn;

    public ScriptRunner(PigServer ps, Connection c) {
	svr = ps;
	conn = c;
    }

    public void dumpScripts(List<Script> scripts, PrintStream pw) {
	for (Script s : scripts) {
	    s.print(pw);
	}
    }

    public void runScripts(List<Script> scripts) 
	throws IOException, FrontendException, ExecException, SQLException {
	runScripts(scripts,true);
    }
    public void runScripts(List<Script> scripts, boolean runPig)
	throws IOException, FrontendException, ExecException, SQLException {
	for (Script s : scripts) {
	    log.info("Running script "+s.getName());
	    switch (s.getType()) {
	    case PIG:
		if (runPig) runPigScript(s);
		break;
	    case SQL:
		runSQLScript(s);
		break;
	    default:
		// huh?
	    }
	}
    }

    public void runPigScript(Script s) 
	throws IOException, FrontendException, ExecException {
	
	long start = System.currentTimeMillis();

	svr.setBatchOn(); // we should be in batch mode, but...

	for (String sstmt : s.getStatements()) {
	    svr.registerQuery(sstmt);
	}
	svr.executeBatch();

	long end = System.currentTimeMillis();
	log.info("Time: "+(end-start)+"ms");
    }

    public void runSQLScript(Script s) throws SQLException {
	long start = System.currentTimeMillis();
	Statement jstmt = conn.createStatement();
	for (String sstmt : s.getStatements()) {
	    try {
		jstmt.execute(sstmt);
	    } catch (SQLException e) {
		if (s.isAllowedToFail()) {
		    log.warn(e.getMessage());
		} else throw e;
	    }
	}
	jstmt.close();
	long end = System.currentTimeMillis();
	log.info("Time: "+(end-start)+"ms");
    }
}
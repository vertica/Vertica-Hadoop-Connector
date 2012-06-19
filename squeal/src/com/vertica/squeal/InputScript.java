/* Copyright (c) 2005 - 2012 Vertica, an HP company -*- Java -*- */
package com.vertica.squeal;

import java.io.*;
import java.util.*;

import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.LogicalPlanBuilder;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.logicalLayer.parser.QueryParser;
import org.apache.pig.impl.plan.OperatorKey;

public class InputScript {
    private Map<LogicalOperator, LogicalPlan> aliases = new HashMap<LogicalOperator, LogicalPlan>();
    
    private Map<OperatorKey, LogicalOperator> opTable = new HashMap<OperatorKey, LogicalOperator>();
    
    private Map<String, LogicalOperator> aliasOp = new HashMap<String, LogicalOperator>();
    
    private List<String> scriptCache = new ArrayList<String>();	
    
    // the fileNameMap contains filename to canonical filename
    // mappings. This is done so we can reparse the cached script
    // and remember the translation (current directory might only
    // be correct during the first parse
    private Map<String, String> fileNameMap = new HashMap<String, String>();
    
    private LogicalPlan lp;
    
    private PigContext context;

    public InputScript(PigContext cnxt) { context = cnxt; }

    public void readScript(String fname) throws IOException, ParseException {

	// FileInputStream fis = new FileInputStream(new File(fname));
	// QueryParser parser = new QueryParser(fis,context,"1",aliases,
	// 				     opTable,aliasOp, 1, fileNameMap);
	// LogicalPlan newlp = parser.Parse();
	// while (newlp != null) {
	//     lp = newlp;
	//     newlp = parser.Parse();
	// }

	BufferedReader infile = new BufferedReader(new FileReader(new File(fname)));
	String line = infile.readLine();
	int i=1;
	LogicalPlanBuilder builder = new LogicalPlanBuilder(context);
	while (line != null) {
	    if (line.trim().length() == 0) continue;
	    LogicalPlan tmpLp = builder.parse("1",
					      line,
					      aliases,
					      opTable,
					      aliasOp,
					      i,
					      fileNameMap);
	    lp = tmpLp;
	    line = infile.readLine();
	    i++;
	}
    }

    public LogicalPlan getLogicalPlan() { return lp; }

    public Set<String> getAliases() { return aliasOp.keySet(); }
}
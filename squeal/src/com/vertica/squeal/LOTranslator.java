/* Copyright (c) 2005 - 2012 Vertica, an HP company -*- Java -*- */
package com.vertica.squeal;

import java.io.InputStream;
import java.io.PrintStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.*;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.PropertiesUtil;

public class LOTranslator extends LOVisitor {
    private final static Log log = LogFactory.getLog(LOTranslator.class);

    public static final String SQUEAL_SCHEMA="squeal_";

    public class Production {
	String sql;
	String orderby;
	String predicate;
	boolean isDistinct;
	boolean hasLimit;
	// add schema info here
	boolean grouped;

	public Production(String s) {
	    sql = s;
	}

	public Production(Production src) {
	    sql = src.sql;
	    orderby = src.orderby;
	    predicate = src.predicate;
	    isDistinct = src.isDistinct;
	    hasLimit = src.hasLimit;
	    grouped = src.grouped;
	}
    }

    private String jobName,schemaName;
    private String verticaLoaderString, verticaStorerString;
    private Set<String> aliases, tables;
    private Stack<LogicalPlan> plans;
    private String joinAlias;
    private Map<OperatorKey,Production> prods;
    private Properties funcMap;
    public List<String> queries;
    private int exprContext;
    private boolean collapseNow;

    private List<Script> scripts;

    private Script sqldefnScript, testSqlScript, curScript;

    public LOTranslator() {
	super(null,null);
	setPigConnectorStrings();
	aliases = new HashSet<String>();
	tables = new HashSet<String>();
	plans = new Stack<LogicalPlan>();
	prods = new HashMap<OperatorKey,Production>();

	funcMap = new Properties();
	loadProperties("/verticafuncs.properties");

	queries = new ArrayList<String>();
	scripts = new ArrayList<Script>();
    }

    public List<Script> run(String jn, LogicalPlan plan, Set<String> as, boolean first) 
	throws VisitorException {
	mPlan = plan;
	mCurrentWalker = new DependencyOrderWalker<LogicalOperator, LogicalPlan>(plan);

	aliases.addAll(as);
	joinAlias = null;
	prods.clear();
	queries.clear();
	scripts.clear();
	collapseNow = false;
	exprContext = 0;
	plans.clear();
	plans.push(mPlan);

	jobName = NameUtil.sqlTableNameify(jn);
	schemaName = SQUEAL_SCHEMA+jobName;

	if (first) {
	    Script schemaSetup = new Script("schema refresh",Script.Type.SQL,true);
	    schemaSetup.addStatement("DROP SCHEMA "+schemaName+" CASCADE;");
	    schemaSetup.addStatement("CREATE SCHEMA "+schemaName+";");
	    scripts.add(schemaSetup);
	}

	sqldefnScript = new Script("sql setup",Script.Type.SQL,false);
	scripts.add(sqldefnScript);

	testSqlScript = new Script("test sql",Script.Type.SQL,false);
	scripts.add(testSqlScript);

	schemaName = schemaName+".";

	curScript = null;
	
	// actually generate scripts
	visit();

	return scripts;
    }

    private void loadProperties(String fileName) {
        InputStream inputStream = null;
        Class<PropertiesUtil> clazz = PropertiesUtil.class;
        try {
            inputStream = clazz
                    .getResourceAsStream(fileName);
            if (inputStream == null) {
                String msg = "no " + fileName +
                " configuration file available in the classpath";
                log.debug(msg);
            } else {
                funcMap.load(inputStream);
            }
        } catch (Exception e) {
            log.error("unable to parse " + fileName + " :", e);
        } finally {
            if (inputStream != null) try {inputStream.close();} catch (Exception e) {}
        }
    }

    private void setPigConnectorStrings() {
	StringBuilder sb = new StringBuilder();


    Properties properties = System.getProperties();
    String hostname = properties.getProperty("mapred.vertica.hostnames", "localhost");
    String username = properties.getProperty("mapred.vertica.username", "dbadmin");
    String password = properties.getProperty("mapred.vertica.password", "");
    String database = properties.getProperty("mapred.vertica.database", "");
    String port = properties.getProperty("mapred.vertica.port", "");


	sb.append("com.vertica.pig.VerticaLoader('");
	sb.append(hostname).append("','").append(database);
	sb.append("','").append(port);
	sb.append("','").append(username).append("','").append(password);
	sb.append("');");
	verticaLoaderString = sb.toString();

	sb.setLength(0);

	sb.append("com.vertica.pig.VerticaStorer('");
	sb.append(hostname).append("','").append(database);
	sb.append("','").append(port);
	sb.append("','").append(username).append("','").append(password);
	sb.append("');");
	verticaStorerString = sb.toString();
    }

    private void ensurePigScript(String name) {
	if (curScript != null && curScript.getType() == Script.Type.PIG) return;
	curScript = new Script(name+scripts.size(),Script.Type.PIG,false);
	scripts.add(curScript);
    }
    
    private void ensureSqlScript(String name) {
	if (curScript != null && curScript.getType() == Script.Type.SQL) return;
	curScript = new Script(name+scripts.size(),Script.Type.SQL,false);
	scripts.add(curScript);
    }
    
    private LogicalPlan getTopPlan() {
	return plans.peek();
    }

    private void put(Operator op, String sql) {
	prods.put(op.getOperatorKey(), new Production(sql));
    }

    private Production get(Operator op) {
	return prods.get(op.getOperatorKey());
    }

    private boolean hasInputs(LogicalOperator op) {
	return getTopPlan().getPredecessors(op) != null;
    }

    private Production getInput(LogicalOperator op) {
	return get(getTopPlan().getPredecessors(op).get(0));
    }

    private Production getInput(LogicalOperator op, int i) {
	return get(getTopPlan().getPredecessors(op).get(i));
    }

    private String resolveField(Schema.FieldSchema fs, int i) {
	if (fs.alias != null) {
	    if ("group".equals(fs.alias)) return "pgroup";
	    else return NameUtil.sqlTableNameify(fs.alias);
	}
	else return NameUtil.sqlTableNameify("c"+i);
    }

    private void crossBackFromVertica(String alias, LogicalOperator toMat) {
	Production p = get(toMat);
	String query = "SELECT * FROM "+p.sql;
	testSqlScript.addStatement(query);
	
	ensureSqlScript("populate "+alias);
	String tableName = schemaName+alias;
	StringBuilder sb = new StringBuilder();
	sb.append("CREATE TABLE ").append(tableName).append(" AS ").append(query);
	curScript.addStatement(sb.toString());

	ensurePigScript("store "+alias);
	sb.setLength(0);
	sb.append(alias).append(" = LOAD 'sql://{SELECT * FROM ").append(tableName).append("}' using ").append(verticaLoaderString);
	curScript.addStatement(sb.toString());

	// adjust production, in case anyone else needs it
	p.sql = tableName;
    }

    /**
     * @param lOp
     *            the logical operator that has to be visited
     * @throws VisitorException
     */
    public void visit(LogicalOperator lOp)
	throws VisitorException {
        //
        // Do Nothing
        //
    }

    /**
     * @param eOp
     *            the logical expression operator that has to be visited
     * @throws VisitorException
     */
    public void visit(ExpressionOperator eOp)
	throws VisitorException {
        //
        // Do Nothing
        //
    }

    /**
     * @param binOp
     *            the logical binary expression operator that has to be visited
     * @throws VisitorException
     */
    public void visit(BinaryExpressionOperator binOp)
	throws VisitorException {
        //
        // Visit the left hand side operand followed by the right hand side
        // operand
        //

        /*
	  binOp.getLhsOperand().visit(this);
	  binOp.getRhsOperand().visit(this);
        */
    }

    /**
     * 
     * @param uniOp
     *            the logical unary operator that has to be visited
     * @throws VisitorException
     */
    public void visit(UnaryExpressionOperator uniOp) throws VisitorException {
        // Visit the operand

        /*
	  uniOp.getOperand().visit(this);
        */
    }

    /**
     * 
     * @param cg
     *            the logical cogroup operator that has to be visited
     * @throws VisitorException
     */
    public void visit(LOCogroup cg) throws VisitorException {
	try {
	    boolean inners[] = cg.getInner();
	    StringBuilder joins = new StringBuilder();
	    StringBuilder projlist = new StringBuilder();
	    String lastExpr = null;
	    int aliasCnt=0;

	    String g=null;

	    // Visit each of the inputs of cogroup.
	    MultiMap<LogicalOperator, LogicalPlan> mapGByPlans = cg.getGroupByPlans();
	    int i=0;
	    exprContext++;
	    for(LogicalOperator op: cg.getInputs()) {
		joinAlias = op.getAlias();
		if (get(op).grouped) throw new VisitorException("Nested group currently unsupported");
		for(LogicalPlan lp: mapGByPlans.get(op)) {
		    if (null != lp) {
			plans.push(lp);
			PlanWalker<LogicalOperator, LogicalPlan> w = new DependencyOrderWalker<LogicalOperator, LogicalPlan>(lp);
			pushWalker(w);
			w.walk(this);
			popWalker();
			plans.pop();

			Production p = get(lp.getSingleLeafPlanOutputOp());
			String curExpr = p.sql;
			Schema inputS = op.getSchema();
			projlist.append(curExpr).append(" AS pgroup");
			for (int j=0; j < inputS.size(); j++) {
			    String ifield = NameUtil.sqlTableNameify(inputS.getField(j).alias);
			    projlist.append(",");
			    projlist.append(op.getAlias()).append(".");
			    projlist.append(ifield);
			    projlist.append(" as ");
			    projlist.append(op.getAlias()).append("_").append(ifield);
			    aliasCnt++;
			}
			if (joins.length() > 0) {
			    if (!inners[i]) {
				if (!inners[i-1]) {
				    joins.append(" FULL OUTER");
				} else {
				    joins.append(" LEFT OUTER");
				}
			    } else if (!inners[i-1]) {
				joins.append(" RIGHT OUTER");
			    }
			    joins.append(" JOIN ");
			}
			joins.append(get(op).sql);
			if (lastExpr != null) {
			    joins.append(" ON ").append(lastExpr).append(" = ").append(curExpr);
			}
			lastExpr = curExpr;
			i++;
		    }
		}
	    }
	    
	    joinAlias = null;
	    exprContext--;
	    StringBuilder sb = new StringBuilder();
	    sb.append("(SELECT ").append(projlist.toString()).append(" FROM ");
	    sb.append(joins.toString()).append(") AS ").append(SchemaUtil.getAlias(cg));
	    Production p = new Production(sb.toString());
	    p.grouped = true;
	    prods.put(cg.getOperatorKey(),p);
	} catch (FrontendException e) {
	    e.printStackTrace();
	    throw new VisitorException(e.getMessage());
	}
    }

    /**
     * 
     * @param loj 
     *            the logical join operator that has to be visited
     * @throws VisitorException
     */
    @SuppressWarnings("unchecked")
	public void visit(LOJoin loj) throws VisitorException {
	
	try {
	    boolean inners[] = loj.getInnerFlags();
	    StringBuilder joins = new StringBuilder();
	    StringBuilder projlist = new StringBuilder();
	    String lastExpr = null;
	    Schema outputS = loj.getSchema();
	    int aliasCnt=0;

	    // Visit each of the inputs of join.
	    MultiMap<LogicalOperator, LogicalPlan> mapJoinPlans = loj.getJoinPlans();
	    int i=0;
	    exprContext++;
	    for(LogicalOperator op: loj.getInputs()) {
		joinAlias = op.getAlias();
		Schema inputS = op.getSchema();
		for (int j=0; j < inputS.size(); j++) {
		    if (projlist.length() > 0) projlist.append(",");
		    projlist.append(op.getAlias()).append(".");
		    projlist.append(NameUtil.sqlTableNameify(inputS.getField(j).alias));
		    projlist.append(" as ");
		    projlist.append(NameUtil.sqlTableNameify(outputS.getField(aliasCnt).alias));
		    aliasCnt++;
		}
		for(LogicalPlan lp: mapJoinPlans.get(op)) {
		    if (null != lp) {
			plans.push(lp);
			PlanWalker w = new DependencyOrderWalker(lp);
			pushWalker(w);
			w.walk(this);
			popWalker();
			plans.pop();
		    
			if (joins.length() > 0) {
			    if (!inners[i]) {
				if (!inners[i-1]) {
				    joins.append(" FULL OUTER");
				} else {
				    joins.append(" LEFT OUTER");
				}
			    } else if (!inners[i-1]) {
				joins.append(" RIGHT OUTER");
			    }
			    joins.append(" JOIN ");
			}
			joins.append(get(op).sql);
			Production p = get(lp.getSingleLeafPlanOutputOp());
			String curExpr = p.sql;
			if (lastExpr != null) {
			    joins.append(" ON ").append(lastExpr).append(" = ").append(curExpr);
			}
			lastExpr = curExpr;
			i++;
		    }
		}
	    }
	    exprContext--;
	    joinAlias = null;
	    StringBuilder sb = new StringBuilder("(SELECT ");
	    sb.append(projlist.toString()).append(" FROM ");
	    sb.append(joins).append(") AS ");
	    sb.append(SchemaUtil.getAlias(loj));
	    put(loj,sb.toString());
	} catch (FrontendException e) {
	    throw new VisitorException(e.getMessage());
	}
    }

    /**
     * 
     * @param forEach
     *            the logical foreach operator that has to be visited
     * @throws VisitorException
     */
    public void visit(LOForEach forEach) throws VisitorException {
	if (exprContext > 0) {
	    Production p = new Production(getInput(forEach));
	    String sc = p.sql;
	    exprContext++;
	    for(LogicalPlan lp: forEach.getForEachPlans()) {
		plans.push(lp);
		PlanWalker<LogicalOperator, LogicalPlan> w = new DependencyOrderWalker<LogicalOperator, LogicalPlan>(lp);
		pushWalker(w);
		w.walk(this);
		popWalker();
		plans.pop();

		p.sql = sc+"_"+get(lp.getSingleLeafPlanOutputOp()).sql;
		prods.put(forEach.getOperatorKey(),p);
	    }
	    exprContext--;
	    return;
	}
	try {
	    StringBuilder exprs = new StringBuilder();
	    Schema s = forEach.getSchema();
	    // Visit each of generates projection elements.
	    int i=0;
	    exprContext++;
	    for(LogicalPlan lp: forEach.getForEachPlans()) {
		plans.push(lp);
		PlanWalker<LogicalOperator, LogicalPlan> w = new DependencyOrderWalker<LogicalOperator, LogicalPlan>(lp);
		pushWalker(w);
		w.walk(this);
		popWalker();
		plans.pop();
		
		if (exprs.length() > 0) exprs.append(",");
		exprs.append(get(lp.getSingleLeafPlanOutputOp()).sql).append(" as ").append(resolveField(s.getField(i),i));
		
		i++;
	    }
	    exprContext--;

	    Production p = getInput(forEach);
	    StringBuilder sb = new StringBuilder();
	    sb.append("(SELECT ").append(exprs.toString()).append(" FROM ").append(getInput(forEach).sql);
	    if (p.grouped && collapseNow) {
		sb.append(" GROUP BY pgroup");
	    }
	    sb.append(") AS ").append(SchemaUtil.getAlias(forEach));
	    put(forEach, sb.toString());
	    collapseNow = false;
	} catch (FrontendException e) {
	    throw new VisitorException(e.getMessage());
	}
    }

    /**
     * 
     * @param s
     *            the logical sort operator that has to be visited
     * @throws VisitorException
     */
    public void visit(LOSort s) throws VisitorException {
	StringBuilder exprs = new StringBuilder();
	// Visit the sort function
	exprContext++;
	for(LogicalPlan lp: s.getSortColPlans()) {
	    plans.push(lp);
	    PlanWalker<LogicalOperator, LogicalPlan> w = new DependencyOrderWalker<LogicalOperator, LogicalPlan>(lp);
	    pushWalker(w);
	    w.walk(this);
	    popWalker();
	    plans.pop();
	    
	    if (exprs.length() > 0) exprs.append(",");
	    exprs.append(get(lp.getSingleLeafPlanOutputOp()).sql);
	}
	exprContext--;

	if (exprContext > 0) {
	    Production p = new Production(getInput(s));
	    p.orderby = exprs.toString();
	    prods.put(s.getOperatorKey(),p);
	} else {
	    StringBuilder sb = new StringBuilder();
	    sb.append("(SELECT * FROM ").append(getInput(s).sql).append(" ORDER BY ").append(exprs);
	    sb.append(") AS ").append(SchemaUtil.getAlias(s));
	    put(s,sb.toString());
	}
    }

    public void visit(LOLimit limOp) throws VisitorException {
	if (exprContext > 0) {
	    Production p = new Production(getInput(limOp));
	    p.hasLimit = true;
	    // hard to implement this - must inject ROW_NUMBER() analytic - save for later
	    prods.put(limOp.getOperatorKey(),p);
	} else {
	    StringBuilder sb = new StringBuilder("(SELECT * FROM ");
	    sb.append(getInput(limOp).sql).append(" LIMIT ").append(limOp.getLimit()).append(") AS ");
	    sb.append(SchemaUtil.getAlias(limOp));
	    put(limOp,sb.toString());
	}
    }
    
    public void visit(LOStream stream) throws VisitorException {
	try {
	    String a = NameUtil.uniquify(aliases,"vdata");
	    aliases.add(a);

	    crossBackFromVertica(a,getTopPlan().getPredecessors(stream).get(0));
	    curScript.addStatement(Deparser.deparse(stream,a));
	} catch (FrontendException e) {
	    throw new VisitorException(e.getMessage());
	}
    }

    /**
     * 
     * @param filter
     *            the logical filter operator that has to be visited
     * @throws VisitorException
     */
    public void visit(LOFilter filter) throws VisitorException {
	exprContext++;
        // Visit the condition for the filter followed by the input
	LogicalPlan lp = filter.getComparisonPlan();
	plans.push(lp);
        PlanWalker<LogicalOperator, LogicalPlan> w = new DependencyOrderWalker<LogicalOperator, LogicalPlan>(lp);
        pushWalker(w);
        w.walk(this);
        popWalker();
	plans.pop();
	exprContext--;

	String expr = get(lp.getSingleLeafPlanOutputOp()).sql;

	if (exprContext > 0) {
	    Production p = new Production(getInput(filter));
	    if (p.predicate == null) {
		p.predicate = expr;
	    } else {
		p.predicate = p.predicate + " AND "+expr;
	    }
	    prods.put(filter.getOperatorKey(),p);
	} else {
	    StringBuilder sb = new StringBuilder("(SELECT * FROM ");
	    sb.append(getInput(filter).sql).append(" WHERE ").append(expr).append(") AS ");
	    sb.append(SchemaUtil.getAlias(filter));
	    put(filter,sb.toString());
	}
    }

    /**
     * 
     * @param split
     *            the logical split operator that has to be visited
     * @throws VisitorException
     */
    public void visit(LOSplit split) throws VisitorException {
        // Visit each of split's conditions
        /*
	  for(LogicalOperator logicalOp: split.getOutputs()) {
	  logicalOp.visit(this);
	  }
        */
    }

    /**
     * 
     * @param g
     *            the logical generate operator that has to be visited
     * @throws VisitorException
     */
    public void visit(LOGenerate g) throws VisitorException {
	/*
	StringBuilder exprs = new StringBuilder();

        // Visit the operators that are part of the foreach plan
        for(LogicalPlan lp: g.getGeneratePlans()) {

            PlanWalker<LogicalOperator, LogicalPlan> w = new DependencyOrderWalker<LogicalOperator, LogicalPlan>(lp);
            pushWalker(w);
            w.walk(this);
            popWalker();

	    String expr = partials.pop();
	    if (exprs.length() > 0) exprs.append(",");
	    exprs.append(expr);
        }

	StringBuilder sb = new StringBuilder();
	sb.append("(SELECT ").append(exprs).append(" FROM ").append(getInput(g).sql).append(") AS ");
	sb.append(SchemaUtil.getAlias(g));
	put(g,sb.toString());
	*/
    }

    public void visit(LOLoad load) throws VisitorException{
	try {
	    String a = load.getAlias();
	    if (a == null) {
		// if load was anonymous, make up a unique alias for it
		a = NameUtil.uniquify(aliases,"pload");
		aliases.add(a);
	    }

	    String n = schemaName+NameUtil.sqlTableNameify(load.getInputFile().getFileName());
	    if (!tables.contains(n)) {
		tables.add(n);
		Schema s = load.getSchema();

		// build table DDL
		//sqldefnScript.addStatement("DROP TABLE "+n+" CASCADE;");
		sqldefnScript.addStatement(SchemaUtil.getTableFromSchema(n,s,false));

		// original load
		ensurePigScript("load");
		curScript.addStatement(Deparser.deparse(load,a));
		// push data to vertica
		curScript.addStatement(SchemaUtil.populateVerticaFromSchema(a,n,s)
				       +verticaStorerString);
	    }	    
	    put(load,n+" as "+a);
	} catch (FrontendException e) {
	    throw new VisitorException(e.getMessage());
	}
    }

    public void visit(LOStore store) throws VisitorException{
	try {
	    String a = NameUtil.uniquify(aliases,"vdata");
	    aliases.add(a);

	    crossBackFromVertica(a,getTopPlan().getPredecessors(store).get(0));
	    curScript.addStatement(Deparser.deparse(store,a));
	} catch (FrontendException e) {
	    throw new VisitorException(e.getMessage());
	}
    }
    
    public void visit(LOUnion u) throws VisitorException {
	throw new VisitorException("unsupported");
    }

    public void visit(LOSplitOutput sop) throws VisitorException {
        LogicalPlan lp = sop.getConditionPlan();
        if (null != lp) {
            PlanWalker<LogicalOperator, LogicalPlan> w = new DependencyOrderWalker<LogicalOperator, LogicalPlan>(lp);
            pushWalker(w);
            w.walk(this);
            popWalker();
        }
    }

    public void visit(LODistinct dt) throws VisitorException {
	if (exprContext > 0) {
	    Production p = new Production(getInput(dt));
	    p.isDistinct = true;
	    prods.put(dt.getOperatorKey(),p);
	} else {
	    StringBuilder sb = new StringBuilder();
	    sb.append("(SELECT DISTINCT * FROM ").append(getInput(dt).sql).append(") as ");
	    sb.append(SchemaUtil.getAlias(dt));
	    put(dt, sb.toString());
	}
    }

    public void visit(LOCross cs) throws VisitorException {
	StringBuilder sb = new StringBuilder();
	sb.append("(SELECT * FROM ");
	int i=0;
	for (LogicalOperator op: cs.getInputs()) {
	    if (i > 0) sb.append(",");
	    sb.append(get(op));
	    i++;
	}
	sb.append(") as ").append(SchemaUtil.getAlias(cs));
	put(cs, sb.toString());
    }

    /**
     * Iterate over each expression that is part of the function argument list
     * 
     * @param func
     *            the user defined function
     * @throws VisitorException
     */
    public void visit(LOUserFunc func) throws VisitorException {
	StringBuilder args = new StringBuilder();
	int nargs = func.getArguments().size();
	boolean isDistinct = false;
	String orderby = null;
	boolean hasLimit = false;
	for (int i=0; i < nargs; i++) {
	    Production p = getInput(func,i);

	    if (args.length() > 0) args.append(",");
	    isDistinct |= p.isDistinct;
	    hasLimit |= p.hasLimit;
	    if (p.orderby != null) {
		if (orderby != null && !p.orderby.equals(orderby))
		    throw new VisitorException("only one orderby allowed");
		orderby = p.orderby;
	    }
	    if (p.predicate != null) {
		args.append("CASE WHEN ").append(p.predicate).append(" THEN ").append(p.sql);
		args.append(" ELSE null END");
	    } else {
		args.append(p.sql);
	    }
	}
	String f = func.getFuncSpec().getClassName();
	String vf = funcMap.getProperty(f);
	if (vf != null && vf != "") {
	    StringBuilder sb = new StringBuilder();
	    sb.append(vf).append("(");
	    if (isDistinct) sb.append("DISTINCT ");
	    sb.append(args).append(")");
	    put(func,sb.toString());
	} else {
	    throw new VisitorException("Function without mapping: "+f);
	}
    }

    /**
     * @param binCond
     *            the logical binCond operator that has to be visited
     * @throws VisitorException
     */
    public void visit(LOBinCond binCond) throws VisitorException {

    }

    /**
     * 
     * @param cast
     *            the logical cast operator that has to be visited
     * @throws VisitorException
     */
    public void visit(LOCast cast) throws VisitorException {

    }
    
    /**
     * 
     * @param regexp
     *            the logical regexp operator that has to be visited
     * @throws ParseException
     */
    public void visit(LORegexp regexp) throws VisitorException {

    }

    public void visit(LOConst c) throws VisitorException{
	try {
	    Schema.FieldSchema fs = c.getFieldSchema();
	    if (fs.type == DataType.CHARARRAY || fs.type == DataType.BYTEARRAY) {
		// TODO: should escape the string value here
		put(c,"'"+c.getValue()+"'");
	    } else {
		put(c,c.getValue().toString());
	    }
	} catch (FrontendException e) {
	    throw new VisitorException(e.getMessage());
	}
    }

    public void visit(LOProject project) throws VisitorException {
	if (hasInputs(project)) {
	    Production p = getInput(project);
	    if (project.getType() == DataType.BAG) collapseNow = true;
	    prods.put(project.getOperatorKey(),p); // pass through unchanged
	    //log.info("Inputs LOProject: "+project.getOperatorKey()+" "+p.sql);
	    return;
	}
        // Visit the operand of the project as long as the sentinel is false
	/*
        if(!project.getSentinel()) {
            project.getExpression().visit(this);
	    log.info("w/ sentinel output: "+partials.peek());
        }
	*/
	
	try {
	    String alias;
	    if (project.getType() == DataType.BAG) {
		collapseNow = true;
		alias = "*";
	    } else {
		alias = resolveField(project.getFieldSchema(),999);
	    }
	    if (joinAlias != null) {
		alias = joinAlias+"."+alias;
	    } else if (project.getExpression() instanceof LOProject) {
		Schema.FieldSchema fs = ((LOProject)project.getExpression()).getFieldSchema();
		if (fs.alias != null) alias = fs.alias+"_"+alias;
	    }
	    if ("group".equals(alias)) alias = "pgroup";
	    put(project,alias);
	    //log.info("Noinputs LOProject: "+project.getOperatorKey()+" "+alias);
	} catch (FrontendException e) {
	    throw new VisitorException(e.getMessage());
	}
    }
    
    public void visit(LOGreaterThan op) throws VisitorException {
	String rhs = getInput(op,1).sql;
	String lhs = getInput(op,0).sql;
	put(op,lhs+" > "+rhs);
	return;
    }

    public void visit(LOLesserThan op) throws VisitorException {
	String rhs = getInput(op,1).sql;
	String lhs = getInput(op,0).sql;
	put(op,lhs+" < "+rhs);
	return;
    }

    public void visit(LOGreaterThanEqual op) throws VisitorException {
	String rhs = getInput(op,1).sql;
	String lhs = getInput(op,0).sql;
	put(op,lhs+" >= "+rhs);
	return;
    }

    public void visit(LOLesserThanEqual op) throws VisitorException {
	String rhs = getInput(op,1).sql;
	String lhs = getInput(op,0).sql;
	put(op,lhs+" <= "+rhs);
	return;
    }

    public void visit(LOEqual op) throws VisitorException {
	String rhs = getInput(op,1).sql;
	String lhs = getInput(op,0).sql;
	put(op,lhs+" = "+rhs);
	return;
    }

    public void visit(LONotEqual op) throws VisitorException {
	String rhs = getInput(op,1).sql;
	String lhs = getInput(op,0).sql;
	put(op,lhs+" != "+rhs);
	return;
    }

    public void visit(LOAdd op) throws VisitorException {
	String rhs = getInput(op,1).sql;
	String lhs = getInput(op,0).sql;
	put(op,lhs+" + "+rhs);
	return;
    }

    public void visit(LOSubtract op) throws VisitorException {
	String rhs = getInput(op,1).sql;
	String lhs = getInput(op,0).sql;
	put(op,lhs+" - "+rhs);
	return;
    }

    public void visit(LOMultiply op) throws VisitorException {
	String rhs = getInput(op,1).sql;
	String lhs = getInput(op,0).sql;
	put(op,lhs+" * "+rhs);
	return;
    }

    public void visit(LODivide op) throws VisitorException {
	String rhs = getInput(op,1).sql;
	String lhs = getInput(op,0).sql;
	put(op,lhs+" / "+rhs);
	return;
    }

    public void visit(LOMod op) throws VisitorException {
	String rhs = getInput(op,1).sql;
	String lhs = getInput(op,0).sql;
	put(op,lhs+" % "+rhs);
	return;
    }

    public void visit(LONegative op) throws VisitorException {
	String exp = getInput(op).sql;
	put(op,"- "+exp);
	return;
    }

    public void visit(LOMapLookup op) throws VisitorException {
	return;
    }

    public void visit(LOAnd binOp) throws VisitorException {
	String rhs = getInput(binOp,1).sql;
	String lhs = getInput(binOp,0).sql;
	put(binOp,lhs+" and "+rhs);
	return;
    }

    public void visit(LOOr binOp) throws VisitorException {
	String rhs = getInput(binOp,1).sql;
	String lhs = getInput(binOp,0).sql;
	put(binOp,lhs+" or "+rhs);
	return;
    }

    public void visit(LONot uniOp) throws VisitorException {
	String exp = getInput(uniOp).sql;
	put(uniOp,"not "+exp);
	return;
    }

    public void visit(LOIsNull uniOp) throws VisitorException {
	String exp = getInput(uniOp).sql;
	put(uniOp,exp+" is null");
	return;
    }
}

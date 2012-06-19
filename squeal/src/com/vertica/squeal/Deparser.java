/* Copyright (c) 2005 - 2012 Vertica, an HP company -*- Java -*- */
package com.vertica.squeal;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.*;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.ProjectionMap;
import org.apache.pig.impl.plan.RequiredFields;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * goes from LogicalOperator -> String.  Should eventually use visitor code.
 */
public class Deparser {
    private final static Log log = LogFactory.getLog(Deparser.class);

    public static final String INTEGER_TYPE = "int";
    public static final String LONG_TYPE = "long";
    public static final String FLOAT_TYPE = "float";
    public static final String DOUBLE_TYPE = "double";
    public static final String BYTEARRAY_TYPE = "bytearray";
    public static final String CHARARRAY_TYPE = "chararray";
    
    public static String deparse(LOLoad load,String alias) throws FrontendException {
	StringBuilder sb = new StringBuilder();
	FileSpec fs = load.getInputFile();
	sb.append(alias).append(" = LOAD '").append(fs.getFileName()).append("' USING ");
	sb.append(fs.getFuncSpec().getClassName()).append("(");
	String[] args = fs.getFuncSpec().getCtorArgs();
	if (args != null && args.length > 0) {
	    sb.append("'");
	    stringJoin(sb,args,"','");
	    sb.append("'");
	}
	sb.append(") ");
	sb.append(getPigSchemaAsString(load.getSchema()));
	sb.append(";");
	return sb.toString();
    }

    public static String deparse(LOStore store, String dataSrc) throws FrontendException {
	StringBuilder sb = new StringBuilder();
	FileSpec fs = store.getOutputFile();
	sb.append("STORE ").append(dataSrc).append(" INTO '");
	sb.append(fs.getFileName()).append("' USING ");
	sb.append(fs.getFuncSpec().getClassName()).append("(");
	String[] args = fs.getFuncSpec().getCtorArgs();
	if (args != null && args.length > 0) {
	    sb.append("'");
	    stringJoin(sb,args,"','");
	    sb.append("'");
	}
	sb.append(");");
	return sb.toString();
    }

    public static String deparse(LOStream stream, String dataSrc) throws FrontendException {
	StringBuilder sb = new StringBuilder();
	sb.append("STREAM ").append(dataSrc).append(" THROUGH '");
	for (String arg : stream.getStreamingCommand().getCommandArgs()) {
	    sb.append(arg).append(" ");
	}
	sb.setLength(sb.length()-1);
	sb.append("';");
	return sb.toString();
    }

	public static String getPigSchemaAsString(Schema s) throws FrontendException {
		StringBuilder sb = new StringBuilder("AS (");
		int nf = s.size();
		for (int i=0; i < nf; i++) {
			Schema.FieldSchema fs = s.getField(i);
			getColumnDefinitionFromField(fs,sb,i,":");
			if (i < nf-1) sb.append(",");
		}
		sb.append(")");
		return sb.toString();
	}

    private static void getColumnDefinitionFromField(Schema.FieldSchema fs, StringBuilder sb, int index, String sep)
	throws FrontendException {

	if (fs.alias == null) {
	    sb.append("c").append(index);
	} else {
	    sb.append(fs.alias);
	}
	sb.append(sep);
	switch (fs.type) {
	case DataType.INTEGER:
	    sb.append(INTEGER_TYPE);
	    break;
	case DataType.LONG:
	    sb.append(LONG_TYPE);
	    break;
	case DataType.FLOAT:
	    sb.append(FLOAT_TYPE);
	    break;
	case DataType.DOUBLE:
	    sb.append(DOUBLE_TYPE);
	    break;
	case DataType.BYTEARRAY:
	    sb.append(BYTEARRAY_TYPE);
	    break;
	case DataType.CHARARRAY:
	    sb.append(CHARARRAY_TYPE);
	    break;
	default:
	    throw new FrontendException("Unsuppported type in load");
	}
    }

    public static StringBuilder stringJoin(StringBuilder sb, String[] ss, String connector)
    {
	for (String s : ss) {
	    sb.append(s).append(connector);
	}
	sb.setLength(sb.length()-connector.length()); // trim last ,
	return sb;
    }

}
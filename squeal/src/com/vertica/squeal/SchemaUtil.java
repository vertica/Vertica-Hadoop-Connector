package com.vertica.squeal;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.schema.*;

public class SchemaUtil {

    public static final String INTEGER_TYPE = "int";
    public static final String LONG_TYPE = "int";
    public static final String FLOAT_TYPE = "double";
    public static final String DOUBLE_TYPE = "double";
    public static final String BYTEARRAY_TYPE = "varbinary(65000)";
    public static final String CHARARRAY_TYPE = "varchar(65000)";

    public static String getTableFromSchema(String name, Schema s, boolean useTemp) 
	throws FrontendException{
	StringBuilder sb = new StringBuilder("CREATE ");
	if (useTemp) sb.append("TEMP ");
	sb.append("TABLE ");
	sb.append(name).append(" (\n");
	int nf = s.size();
	for (int i=0; i < nf; i++) {
	    Schema.FieldSchema fs = s.getField(i);
	    getColumnDefinitionFromField(fs,sb,i);
	    if (i < nf-1) sb.append(",\n");
	    else sb.append("\n");
	}
	sb.append(")");
	if (useTemp) sb.append(" ON COMMIT PRESERVE ROWS");
	sb.append(";");
	return sb.toString();
    }

    public static String populateVerticaFromSchema(String alias, String tableName, Schema s)
	throws FrontendException {
	
	StringBuilder sb = new StringBuilder("STORE ");
	sb.append(alias).append(" into '{").append(tableName).append("(");
	int nf = s.size();
	for (int i=0; i < nf; i++) {
	    Schema.FieldSchema fs = s.getField(i);
	    getColumnDefinitionFromField(fs,sb,i);
	    if (i < nf-1) sb.append(",");
	}
	sb.append(")}' using ");
	return sb.toString();
    }

    public static String getAlias(LogicalOperator op) {
	return op.getAlias() == null ? "_zzz" : op.getAlias();
    }

    private static void getColumnDefinitionFromField(Schema.FieldSchema fs, StringBuilder sb, int index)
	throws FrontendException {

	if (fs.alias == null) {
	    sb.append("c").append(index);
	} else {
	    sb.append(fs.alias);
	}
	sb.append(" ");
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
}
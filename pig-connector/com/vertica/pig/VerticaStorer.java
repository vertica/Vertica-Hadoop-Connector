/* Copyright (c) 2005 - 2012 Vertica, an HP company -*- Java -*- */

package com.vertica.pig;

import java.util.List;
import java.util.ArrayList;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Job;

import org.apache.pig.StoreFunc;
import org.apache.pig.StoreMetadata;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;

import com.vertica.hadoop.VerticaConfiguration;
import com.vertica.hadoop.VerticaOutputFormat;
import com.vertica.hadoop.VerticaRecordWriter;
import com.vertica.hadoop.VerticaRecord;

public class VerticaStorer extends StoreFunc implements StoreMetadata {
	private String [] hostnames = null;
	private String database = null;
	private String port = null;
	private String username = null;
	private String password = null;
	
	private VerticaOutputFormat outputFormat = new VerticaOutputFormat();

	private String tableName = null;
	private String tableDef = null;

	private VerticaRecordWriter writer = null;

	public VerticaStorer(String hostnames, String db, String port, String username, 
			String password) {
		this.hostnames = hostnames.split(",");
		this.database = db;
		this.port = port;
		this.username = username;
		this.password = password;
	}

	//Important to override because of PIG-1378
	public String relToAbsPathForStorage(String location, Path curdir) {
		return location;
	}
    @Override
    public OutputFormat getOutputFormat() throws IOException {
        return outputFormat;
    }

	@Override
	public void prepareToWrite(RecordWriter writer) {
		this.writer = (VerticaRecordWriter) writer;
	}

	@Override
	public void setStoreLocation(String location, Job job) 
			throws IOException {
		String[] locs = location.split("\\{");
	    String tableNameDef = locs[locs.length - 1];
    	tableNameDef = tableNameDef.substring(0, tableNameDef.lastIndexOf('}'));

		tableName = tableNameDef;
		int cols = tableNameDef.indexOf('(');
		if(cols != -1) {
			tableName = tableNameDef.substring(0, cols);
			tableDef = tableNameDef.substring(cols + 1, tableNameDef.length() - 1);
		}
		Configuration conf = job.getConfiguration();
		VerticaConfiguration.configureVertica(conf, hostnames, database, port, username, password);
		if (tableDef != null) {
			String [] def = tableDef.split(",");
			outputFormat.setOutput(job, tableName, false, def);
		} else {
			outputFormat.setOutput(job, tableName);
		}
	}

	@Override
	public void checkSchema(ResourceSchema schema) {
		//TODO: Check column data types too.
		//For now just make sure the table exists & if not 
	}

	public void storeSchema(ResourceSchema schema, String location, Job job) {
	}

	public void storeStatistics(ResourceStatistics stats, String location, Job job) {
	}
  	
	@Override
	public void putNext(Tuple f) throws IOException,org.apache.pig.backend.executionengine.ExecException {
		VerticaRecord record = new VerticaRecord();

		for(int i = 0; i < f.size(); i++) {
			Object obj = f.get(i);
			switch (f.getType(i)) {
				case DataType.BOOLEAN:
					record.add((Boolean)obj);
					break;
				case DataType.FLOAT:
					record.add((Float)obj);
					break;
				case DataType.DOUBLE:
					record.add((Double)obj);
					break;
				case DataType.LONG:
				case DataType.INTEGER:
					record.add(new Long(((Number)obj).longValue()));
					break;
				case DataType.BYTE:
				case DataType.BYTEARRAY:
					record.add(((DataByteArray)obj).get());
					break;
				case DataType.CHARARRAY:
				case DataType.BIGCHARARRAY:
					record.add((String)obj);
					break;
				case DataType.NULL: 
					record.add(null);
					break;
				default:
					throw new IOException("Vertica connector does not support " 
							+ DataType.findTypeName(f.getType(i)));
			}
		}
		writer.write(null, record);
  	}
}

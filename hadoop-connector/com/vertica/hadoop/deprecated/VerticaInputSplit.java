/* Copyright (c) 2005 - 2012 Vertica, an HP company -*- Java -*- */

package com.vertica.hadoop.deprecated;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import com.vertica.hadoop.VerticaConfiguration;
import com.vertica.hadoop.VerticaRecord;

public class VerticaInputSplit implements InputSplit {
	private static final Log LOG = LogFactory.getLog("com.vertica.hadoop");
	
	PreparedStatement stmt = null;
	Connection connection = null;
	VerticaConfiguration vtconfig = null;
	String inputQuery = null;
	List<Object> segmentParams = null;
	long start = 0;
	long end = 0;
	
	public VerticaInputSplit() { LOG.info("Input split default constructor"); }
	
	public VerticaInputSplit(String input_query, List<Object> segment_params, long start, long end ) {
		LOG.info("Input split constructor with query and params");
		this.inputQuery = input_query;
		this.segmentParams = segment_params;
		this.start = start;
		this.end = end;
	}

	public void configure(JobConf job) throws Exception {
		LOG.info("Input split configured");
		vtconfig = new VerticaConfiguration(job);
		connection = vtconfig.getConnection(false);
		connection.setAutoCommit(true);
		connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
	}
	
	public List<Object> getSegmentParams() { return segmentParams; }
	
	public ResultSet executeQuery() throws Exception {
		LOG.info("Input split execute query");
		long length = getLength();
		
		if(length != 0) inputQuery = "SELECT * FROM ( " + inputQuery + " ) limited LIMIT ? OFFSET ?";
		
		if(connection == null) throw new Exception("Cannot execute query with no connection");
		stmt = connection.prepareStatement(inputQuery);
		
		int i = 1;
		if(segmentParams != null)
			for(Object param : segmentParams) stmt.setObject(i++, param);
		
		if(length != 0) {
			stmt.setLong(i++, length);
			stmt.setLong(i++, start);
		}
		
		ResultSet rs = stmt.executeQuery();
		return rs;
	}
	
	public void close() throws SQLException {
		stmt.close();
	}
	
	public long getStart() {
		return start;
	}
	
	@Override
	public long getLength() throws IOException {
		//TODO: figureout how to return length when there is no start and end
		return end - start;
	}

	@Override
	public String[] getLocations() throws IOException {
		return new String[] {};
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
	    inputQuery = Text.readString(in);
    	long paramCount = in.readLong();
		if (paramCount > 0) {
			int type = in.readInt();
			segmentParams = new ArrayList<Object>();
    		for (int i = 0; i < paramCount; i++) {
				segmentParams.add(VerticaRecord.readField(type, in));
			}
		}
		start = in.readLong();
		end = in.readLong();
	}
 
  	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, inputQuery);
		if (segmentParams != null && segmentParams.size() > 0) {
			out.writeLong(segmentParams.size());
			int type = VerticaRecord.getType(segmentParams.get(0));
			out.writeInt(type);
			for (Object o : segmentParams)
				VerticaRecord.write(o, type, out);
		} else {
			out.writeLong(0);
		}

    	out.writeLong(start);
	    out.writeLong(end);
	}
}

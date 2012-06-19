/* Copyright (c) 2005 - 2012 Vertica, an HP company -*- Java -*- */

package com.vertica.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * Input split class for reading data from Vertica
 * 
 */
public class VerticaInputSplit extends InputSplit implements Writable {
  private static final Log LOG = LogFactory.getLog("com.vertica.hadoop");

  PreparedStatement stmt = null;
  Connection connection = null;
  VerticaConfiguration vtconfig = null;
  String inputQuery = null;
  List<Object> segmentParams = null;
  long start = 0;
  long end = 0;

  /** (@inheritDoc) */
  public VerticaInputSplit() {
    LOG.trace("Input split default constructor");
  }

	/**
	  * Set the input query and a list of parameters to substitute when evaluating
	  * the query
	  * 
	  * @param inputQuery
	  *          SQL query to run
	  * @param segmentParams
	  *          list of parameters to substitute into the query
	  * @param start
	  *          the logical starting record number
	  * @param end
	  *          the logical ending record number
	  */
	public VerticaInputSplit(String inputQuery, List<Object> segmentParams) {
		if (LOG.isDebugEnabled())
		{
			StringBuilder sb = new StringBuilder();
			sb.append("Input split with query -");
			sb.append(inputQuery);
			sb.append("-, Parameters: ");

			boolean addComma = false;
			for (Object param : segmentParams) {
				if (addComma)
					sb.append(",");
				sb.append(param.toString());
			}
			LOG.debug(sb.toString());
		}

		this.inputQuery = inputQuery;
		this.segmentParams = segmentParams;
	}

	public VerticaInputSplit(String inputQuery, long start, long end) {
		LOG.debug("Input split with query -"+inputQuery+"-, start row: " 
				+ start + " and end row: " + end);
		this.inputQuery = inputQuery;
		this.start = start;
		this.end = end;
	}

  /** (@inheritDoc) */
  public void configure(Configuration conf) throws Exception {
    LOG.trace("Input split configured");
    vtconfig = new VerticaConfiguration(conf);
    connection = vtconfig.getConnection(false);
    connection.setAutoCommit(true);
    connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
  }

  /**
   * Return the parameters used for input query
   * 
   * @return
   */
  public List<Object> getSegmentParams() {
    return segmentParams;
  }

	/**
	 * Run the query that, when executed returns input for the mapper
	 * 
	 * @return
	 * @throws Exception
	 */
	public ResultSet executeQuery() throws Exception {
		LOG.trace("Input split execute query");

		if (connection == null)
			throw new Exception("Cannot execute query with no connection");

		if (segmentParams != null) {
			LOG.debug("Query:" + inputQuery + ". No. of params = " + segmentParams.size());
			stmt = connection.prepareStatement(inputQuery);
			int i = 1;
			for (Object param : segmentParams)
			{
				stmt.setObject(i++, param);
				LOG.debug("With param :" + param.toString());
			}
		}

		long length = getLength();
		if (length != 0)
		{
			String query = "SELECT * FROM ( " + inputQuery
				+ " ) limited LIMIT " + length + " OFFSET " + start;
			LOG.debug("Query:" + query);
			stmt = connection.prepareStatement(query);
		}

		LOG.debug("Executing query");
		ResultSet rs = stmt.executeQuery();
		return rs;
	}

  /** (@inheritDoc) */
  public void close() throws SQLException {
    stmt.close();
  }

  /**
   * @return The index of the first row to select
   */
  public long getStart() {
    return start;
  }

  /**
   * @return The index of the last row to select
   */
  public long getEnd() {
    return end;
  }

  /**
   * @return The total row count in this split
   */
  public long getLength() throws IOException {
    // TODO: figureout how to return length when there is no start and end
    return end - start;
  }

  /** {@inheritDoc} */
  public String[] getLocations() throws IOException {
    return new String[] {};
  }

  /** (@inheritDoc) */
  public Configuration getConfiguration() {
    return vtconfig.getConfiguration();
  }


	@Override
	public void readFields(DataInput in) throws IOException {
	    inputQuery = Text.readString(in);
		segmentParams = null;
    	long paramCount = in.readLong();
		LOG.debug("Reading " + paramCount + " parameters");
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
			LOG.debug("Writing out " + segmentParams.size() + " parameters");
			out.writeLong(segmentParams.size());
			int type = VerticaRecord.getType(segmentParams.get(0));
			out.writeInt(type);
			for (Object o : segmentParams)
				VerticaRecord.write(o, type, out);
		} else {
			LOG.debug("Writing out no parameters");
			out.writeLong(0);
		}

    	out.writeLong(start);
	    out.writeLong(end);
	}
}

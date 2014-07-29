/* Copyright (c) 2005 - 2012 Vertica, an HP company -*- Java -*- */

package com.vertica.hadoop;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
  
/**
 * Input formatter that returns the results of a query executed against Vertica.
 * The key is a record number within the result set of each mapper The value is
 * a VerticaRecord, which uses a similar interface to JDBC ResultSets for
 * returning values.
 * 
 */
public class VerticaInputFormat extends InputFormat<LongWritable, VerticaRecord> {
  	private static final Log LOG = LogFactory.getLog("com.vertica.hadoop");
	private String inputQuery = null;
	private String params = null;

	public VerticaInputFormat() {}
	/**
	  * Set a parameterized input query for a job and the query that returns the
	  * parameters.
	  * 
	  * @param query
	  *          SQL query that has parameters specified by question marks ("?")
	  * @param params 
	  *          SQL query that returns parameters for the input query or
	  *          the parameters to substiture
	  */

	public VerticaInputFormat(String query, String params) {
		inputQuery = query;
		this.params = params;
	}

	/**
	 * Set the input query for a job
	 * 
	 * @param job
	 * @param inputQuery
	 *          query to run against Vertica
	 */
	public static void setInput(Job job, String inputQuery) {
		job.setInputFormatClass(VerticaInputFormat.class);
		VerticaConfiguration config = 
			new VerticaConfiguration(job.getConfiguration());
		config.setInputQuery(inputQuery);
	}

	/**
	  * Set a parameterized input query for a job and the query that returns the
	  * parameters.
	  * 
	  * @param job
	  * @param inputQuery
	  *          SQL query that has parameters specified by question marks ("?")
	  * @param segmentParamsQuery
	  *          SQL query that returns parameters for the input query
	  */
  
	public static void setInput(Job job, String inputQuery,
			String segmentParamsQuery) {
		job.setInputFormatClass(VerticaInputFormat.class);
		VerticaConfiguration config = 
			new VerticaConfiguration(job.getConfiguration());
		config.setInputQuery(inputQuery);
		config.setParamsQuery(segmentParamsQuery);
	}

	/**
	  * Set the input query and any number of comma delimited literal list of
	  * parameters
	  * 
	  * @param job
	  * @param inputQuery
	  *          SQL query that has parameters specified by question marks ("?")
	  * @param segmentParams
	  *          any numer of comma delimited strings with literal parameters to
	  *          substitute in the input query
	  */
  
	@SuppressWarnings("serial")
	public static void setInput(Job job, String inputQuery, 
			String... segmentParams) throws IOException {
		// transform each param set into array
		DateFormat datefmt = DateFormat.getDateInstance();
		Collection<List<Object>> params = new HashSet<List<Object>>() {};
		for (String strParams : segmentParams) {
			List<Object> param = new ArrayList<Object>();

			for (String strParam : strParams.split(",")) {
				strParam = strParam.trim();
				if (strParam.charAt(0) == '\''
						&& strParam.charAt(strParam.length() - 1) == '\'')
					param.add(strParam.substring(1, strParam.length() - 1));
				else {
					try {
						param.add(datefmt.parse(strParam));
					} catch (ParseException e1) {
						try {
							param.add(Integer.parseInt(strParam));
						} catch (NumberFormatException e2) {
							throw new IOException("Error parsing argument " + strParam);
						}
					}
				}
			}

			params.add(param);
		}

		setInput(job, inputQuery, params);
	}

	/**
	  * Set the input query and a collection of parameter lists
	  * 
	  * @param job
	  * @param inpuQuery
	  *          SQL query that has parameters specified by question marks ("?")
	  * @param segmentParams
	  *          collection of ordered lists to subtitute into the input query
	  * @throws IOException
	  */

	public static void setInput(Job job, String inpuQuery,
			Collection<List<Object>> segmentParams) throws IOException {
		job.setInputFormatClass(VerticaInputFormat.class);
		VerticaConfiguration config = new VerticaConfiguration(job.getConfiguration());
		config.setInputQuery(inpuQuery);
		config.setInputParams(segmentParams);
	}

  
	/** {@inheritDoc} */
	public RecordReader<LongWritable, VerticaRecord> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		try {
			return new VerticaRecordReader((VerticaInputSplit) split, 
					context.getConfiguration());
		} catch (Exception e) {
                        throw new IOException(e);
		}
	}

	/** {@inheritDoc} */
  	public List<InputSplit> getSplits(JobContext context) throws IOException {
		Configuration conf = context.getConfiguration();
		long numSplits = conf.getInt("mapreduce.job.maps", 1);
		LOG.debug("creating splits up to " + numSplits);
		List<InputSplit> splits = new ArrayList<InputSplit>();

		int i = 0;

		// This is the fancy part of mapping inputs...here's how we figure out
		// splits
		// get the params query or the params
		VerticaConfiguration config = new VerticaConfiguration(conf);
		
		if (inputQuery == null)
			inputQuery = config.getInputQuery();

		if (inputQuery == null)
			throw new IOException("Vertica input requires query defined by "
					+ VerticaConfiguration.QUERY_PROP);

		if (params == null)
			params = config.getParamsQuery();

		Collection<List<Object>> paramCollection = config.getInputParameters();

		if (params != null && params.startsWith("select")) {
			LOG.debug("creating splits using paramsQuery :" + params);
			Connection conn = null;
			Statement stmt = null;

			try {
				conn = config.getConnection(false);
				stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery(params);
				ResultSetMetaData rsmd = rs.getMetaData();

				while (rs.next()) {
					List<Object> segmentParams = new ArrayList<Object>();
					for (int j = 1; j <= rsmd.getColumnCount(); j++) {
						segmentParams.add(rs.getObject(j));
					}
					splits.add(new VerticaInputSplit(inputQuery, segmentParams));
				}
			} catch (Exception e) {
				throw new IOException(e);
			} finally {
				try {
					if (stmt != null) stmt.close();
				} catch (SQLException e) {
					throw new IOException(e);
				}
			}
		} else if (params != null) {
			LOG.debug("creating splits using " + params + " params");
			for (String strParam : params.split(",")) {
				strParam = strParam.trim();
				if (strParam.charAt(0) == '\''
						&& strParam.charAt(strParam.length() - 1) == '\'')
					strParam = strParam.substring(1, strParam.length() - 1);
				List<Object> segmentParams = new ArrayList<Object>();
				segmentParams.add(strParam);
				splits.add(new VerticaInputSplit(inputQuery, segmentParams));
			}
		} else if (paramCollection != null) {
			LOG.debug("creating splits using " + paramCollection.size() + " params");
			for (List<Object> segmentParams : paramCollection) {
				// if there are more numSplits than params we're going to introduce some
				// limit and offsets
				splits.add(new VerticaInputSplit(inputQuery, segmentParams));
			}
		} else {
			LOG.debug("creating splits using limit and offset");
			Connection conn = null;
			Statement stmt = null;

			long count = 0;
			long start = 0;
			long end = 0;

			// TODO: limit needs order by unique key
			// TODO: what if there are more parameters than numsplits?
			// prep a count(*) wrapper query and then populate the bind params for each
			String countQuery = "SELECT COUNT(*) FROM (\n" + inputQuery + "\n) count";

			try {
				conn = config.getConnection(false);
				stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery(countQuery);
				rs.next();

				count = rs.getLong(1);
			} catch (Exception e) {
				throw new IOException(e);
			} finally {
				try {
					if (stmt != null) stmt.close();
				} catch (SQLException e) {
					throw new IOException(e);
				}
			}

			long splitSize = count / numSplits;
			end = splitSize;

			LOG.debug("creating " + numSplits + " splits for " + count + " records");

			for (i = 1; i < numSplits; i++) {
				splits.add(new VerticaInputSplit(inputQuery, start, end));
				LOG.debug("Split(" + i + "), start:" + start + ", end:" + end);
				start += splitSize;
				end += splitSize;
				count -= splitSize;
			}

			if (count > 0) {
				splits.add(new VerticaInputSplit(inputQuery, start, start + count));
			}
		}

		LOG.debug("returning " + splits.size() + " final splits");
		return splits;
	}
}

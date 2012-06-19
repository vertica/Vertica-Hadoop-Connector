/* Copyright (c) 2005 - 2012 Vertica, an HP company -*- Java -*- */

package com.vertica.pig;

import java.io.IOException;

import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Date;
import java.sql.SQLException;

import java.math.BigDecimal;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

import java.net.URLEncoder;
import java.net.URLDecoder;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;

import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DataByteArray;

import com.vertica.hadoop.VerticaConfiguration;
import com.vertica.hadoop.VerticaInputFormat;
import com.vertica.hadoop.VerticaRecordReader;
import com.vertica.hadoop.VerticaRecord;
import org.apache.pig.impl.util.UDFContext;

public class VerticaLoader extends LoadFunc implements LoadMetadata{
	private String [] hostnames = null;
	private String database = null;
	private String port = null;
	private String username = null;
	private String password = null;

	private String contextSignature = null;

    private RecordReader reader = null;

  	DateFormat datefmt = new SimpleDateFormat("yyyyMMdd");
  	DateFormat timefmt = new SimpleDateFormat("HHmmss");
  	DateFormat tmstmpfmt = new SimpleDateFormat("yyyyMMddHHmmss");
  	DateFormat sqlfmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public VerticaLoader(String hostnames, String db, String port, String username, String password) {
		this.hostnames = hostnames.split(",");
		this.database = db;
		this.port = port;
		this.username = username;
		this.password = password; 
	}

    public void setUDFContextSignature(String signature) {
        this.contextSignature = signature;
    }

	@Override
    public InputFormat getInputFormat() throws IOException {
        return new VerticaInputFormat(getQuery(), getParameters());
    }

	private Tuple translate(VerticaRecord v) throws IOException {
		int columns = v.size();
        Tuple result = TupleFactory.getInstance().newTuple(columns);

		for (int i = 0; i < columns; i++) {
			Object obj = v.get(i);
			Integer type = v.getType(i);

			if (obj == null) {
				result.set(i, null);
				continue;
			}

			switch (type) {
				case Types.NULL:
					result.set(i, null);
					break;
				case Types.BIGINT:
					if (obj instanceof BigDecimal) {
						result.set(i, new Long(((BigDecimal) obj).longValue()));
						break;
					}
				case Types.INTEGER:
				case Types.TINYINT:
				case Types.SMALLINT:
					result.set(i, (Long) obj);
					break;
				case Types.DECIMAL:
				case Types.NUMERIC:
				case Types.REAL:
					if (obj instanceof BigDecimal) {
						result.set(i, new Double(((BigDecimal) obj).doubleValue()));
						break;
					}
				case Types.FLOAT:
				case Types.DOUBLE:
					result.set(i, (Double) obj);
					break;
				case Types.BINARY:
				case Types.LONGVARBINARY:
				case Types.VARBINARY:
					result.set(i, new DataByteArray((byte [])obj));
					break;
				case Types.BIT:
				case Types.BOOLEAN:
					result.set(i, (Boolean) obj);
					break;
				case Types.CHAR:
				case Types.LONGNVARCHAR:
				case Types.LONGVARCHAR:
				case Types.NCHAR:
				case Types.NVARCHAR:
				case Types.VARCHAR:
					result.set(i, (String) obj);
					break;
				case Types.DATE:
					result.set(i, new String(datefmt.format((Date) obj)));
					break;
				case Types.TIME:
					result.set(i, new String(timefmt.format((Time) obj)));
					break;
				case Types.TIMESTAMP:
					result.set(i, new String(tmstmpfmt.format((Timestamp) obj)));
					break;
				default:
					throw new IOException("Unknown type value " + type);
			}
		}
		return result;
	}

    @Override
    public Tuple getNext() throws IOException {
        LongWritable key = null;
        VerticaRecord value = null;

        try {

            if(!reader.nextKeyValue())
                return null;
            key = (LongWritable) reader.getCurrentKey();
            value = (VerticaRecord) reader.getCurrentValue();

        } catch(InterruptedException e) {
            throw new IOException("Error reading in key/value");
        }

        if(key == null || value == null) {
            return null;
        }

        return translate(value);
    }

	@Override
	public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
  		this.reader = reader;
  	}

	@Override
	public ResourceSchema getSchema(String location, Job job) throws IOException
	{
		Configuration conf = job.getConfiguration();
		VerticaConfiguration.configureVertica(conf, hostnames, database, port, username, password);
		Schema schema = new Schema();
    	try {
			VerticaConfiguration vtconfig = new VerticaConfiguration(conf);

			Connection conn = vtconfig.getConnection(false);

			location = java.net.URLDecoder.decode(location, "UTF-8");
			String[] query_args = location.split(";");
			String query = query_args[0].substring("sql://{".length()).replace('}', ' ');
			PreparedStatement stmt = conn.prepareStatement(query);
			ResultSetMetaData rsmd = stmt.getMetaData();
			for(int i = 1; i <= rsmd.getColumnCount(); i++) {
				String name = rsmd.getColumnName(i);
				byte type = 0;

				switch(rsmd.getColumnType(i)) {
					case Types.NULL: type = DataType.NULL; break;
					case Types.BIGINT: type = DataType.LONG; break;
					case Types.INTEGER:
					case Types.TINYINT:
					case Types.SMALLINT: type = DataType.INTEGER; break;
					case Types.REAL:
					case Types.DECIMAL:
					case Types.NUMERIC:
					case Types.DOUBLE: type = DataType.DOUBLE; break;
					case Types.FLOAT: type = DataType.FLOAT; break;
					case Types.BINARY:
					case Types.LONGVARBINARY:
					case Types.VARBINARY:  type = DataType.BYTEARRAY; break;
					case Types.BIT:
					case Types.BOOLEAN: type = DataType.BOOLEAN; break;
					case Types.LONGNVARCHAR:
					case Types.LONGVARCHAR:
					case Types.NCHAR:
					case Types.NVARCHAR:
					case Types.VARCHAR:
					case Types.CHAR: type = DataType.CHARARRAY; break;
					case Types.DATE:
					case Types.TIME:
					case Types.TIMESTAMP: type = DataType.LONG; break;
        		}
        
				FieldSchema field = new FieldSchema(name, type);
				schema.add(field);
      		}
			conn.close();
		} catch (SQLException s) {
			throw new IOException("Failed to connect to Vertica. Please check username/password for Vertica database. ("
					+ s.getMessage() + ")");
		} catch (Exception e) {
			throw new IOException(e);
    	}
   
		return new ResourceSchema(schema);
	}

	public void setQuery(String query) {
    	Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass(),
				new String[] {contextSignature});
		p.setProperty(contextSignature + "_inputQuery", query);
	}
	
	public String getQuery() {
    	Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass(),
				new String[] {contextSignature});
		return p.getProperty(contextSignature + "_inputQuery");
	}

	public void setParameters(String str) {
    	Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass(),
				new String[] {contextSignature});
		p.setProperty(contextSignature + "_parameters", str);
	}
	
	public String getParameters() {
    	Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass(),
				new String[] {contextSignature});
		return p.getProperty(contextSignature + "_parameters");
	}

	@Override
	public void setLocation(String location, Job job) throws IOException {
		Configuration conf = job.getConfiguration();
		conf.setBoolean("pig.noSplitCombination", true);
		location = java.net.URLDecoder.decode(location, "UTF-8");
		String[] query_args = location.split(";");
		String query = query_args[0].substring("sql://{".length()).replace('}', ' ');
		setQuery(query);
		String params = null;
		if (query_args.length > 1)
			params = query_args[1];

		if (params != null && !params.isEmpty()) {
			if (params.startsWith("sql://")) {
				params = params.substring("sql://{".length()).replace('}', ' ');
				setParameters(params);
			} else {
				params  = params.replaceAll("^\\s*\\{","");
				params  = params.replaceAll("\\}\\s*$","");
				setParameters(params);
			}
		}
		VerticaConfiguration.configureVertica(conf, hostnames, database, port, username, password);
	}

	public String[] getPartitionKeys(String location, Job job) {
		return null;
	}

	public void setPartitionFilter(Expression filter) {
	}

	public ResourceStatistics getStatistics(String location, Job job) {
		return null;
	}

	//Important to override because of PIG-1378
	@Override
	public String relativeToAbsolutePath(String location, Path curdir) throws IOException {
		try {
			String enc = java.net.URLEncoder.encode(location, "UTF-8");
			return enc;
		} catch (java.io.UnsupportedEncodingException e) {
			throw new IOException(e);
		}
	}
}

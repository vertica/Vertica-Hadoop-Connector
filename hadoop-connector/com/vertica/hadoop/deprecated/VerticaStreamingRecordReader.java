/* Copyright (c) 2005 - 2012 Vertica, an HP company -*- Java -*- */

package com.vertica.hadoop.deprecated;

import com.vertica.hadoop.VerticaRecord;
import com.vertica.hadoop.VerticaConfiguration;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

public class VerticaStreamingRecordReader implements RecordReader<Text, Text> {
	ResultSet results = null;
	VerticaRecord internal_record = null;
	long start = 0;
	int pos = 0;
	long length = 0;
	VerticaInputSplit split = null;
	String delimiter = VerticaConfiguration.DELIMITER;
	String terminator = VerticaConfiguration.RECORD_TERMINATOR;
	
	public VerticaStreamingRecordReader(VerticaInputSplit split, JobConf job) throws Exception {
		//run query for this segment
		this.split = split;
		split.configure(job);
		start = split.getStart();
		length = split.getLength();
		results = split.executeQuery();
		internal_record = new VerticaRecord(results);
		
		VerticaConfiguration vtconfig = new VerticaConfiguration(job);
		delimiter = vtconfig.getInputDelimiter();
		terminator = vtconfig.getInputRecordTerminator();
	}
	
	@Override
	public void close() throws IOException {
		try {
			split.close();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IOException(e);
		}
	}

	@Override
	public Text createKey() {
		return new Text();
	}

	@Override
	public Text createValue() {
		return new Text();
	}

	@Override
	public long getPos() throws IOException {
		return pos;
	}

	@Override
	public float getProgress() throws IOException {
		// TODO: figure out why length would be 0
		if(length == 0) return 1;
		return pos / length;
	}

	@Override
	public boolean next(Text key, Text value) throws IOException {
		key.set(new Long(pos + start).toString());
		pos++;
		try {
			if (results.next()) {
				StringBuffer sb = new StringBuffer();
				for (int i = 0; i < internal_record.size(); i++) {
					sb.append(internal_record.toString(i, results));

					if (i < internal_record.size())
						sb.append(delimiter);
				}
				sb.append(terminator);
				value.set(sb.toString());
				return true;
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IOException(e.getMessage());
		}
		return false;
	}
}

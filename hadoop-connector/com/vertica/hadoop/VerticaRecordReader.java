/* Copyright (c) 2005 - 2012 Vertica, an HP company -*- Java -*- */

package com.vertica.hadoop;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class VerticaRecordReader extends
    RecordReader<LongWritable, VerticaRecord> {
	ResultSet results = null;
	int nColumns = 0;
	long start = 0;
	int pos = 0;
	long length = 0;
	LongWritable key = null;
	VerticaRecord value = null;
	VerticaInputSplit split = null;

	public VerticaRecordReader(VerticaInputSplit split, Configuration job)
		throws Exception {
		// run query for this segment
		split.configure(job);
		start = split.getStart();
		length = split.getLength();
		results = split.executeQuery();
		nColumns = results.getMetaData().getColumnCount();
		this.split = split;
	}

	/** {@inheritDoc} */
	public void initialize(InputSplit split, TaskAttemptContext context)
		throws IOException, InterruptedException {
		key = new LongWritable();
		try {
			pos++;
			value = new VerticaRecord(results);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IOException(e);
		}
	}

	/** {@inheritDoc} */
	public void close() throws IOException {
		try {
			split.close();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IOException(e);
		}
	}

	/** {@inheritDoc} */
	public float getProgress() throws IOException {
		// TODO: figure out why length would be 0
		if (length == 0) return 1;
		return pos / length;
	}

  	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public VerticaRecord getCurrentValue() throws IOException,
		   InterruptedException {
		return value;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		key.set(pos + start);
		pos++;
		try {
			if (results.next()) {
				for (int i = 0; i < nColumns; i++) {
					value.set(i, results.getObject(i+1));
				}
				return true;
			}
		} catch (SQLException e) {
			throw new IOException(e);
		}
		return false;
	}
}

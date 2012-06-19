/* Copyright (c) 2005 - 2012 Vertica, an HP company -*- Java -*- */

package com.vertica.hadoop.deprecated;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class VerticaStreamingInput implements InputFormat<Text, Text> {

	@Override
	public RecordReader<Text, Text> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException {
		try {
			return new VerticaStreamingRecordReader((VerticaInputSplit)split, job);
		} catch (Exception e) { 
			e.printStackTrace(); 
			return null;
		}
	}

	@Override
	public InputSplit[] getSplits(JobConf job, int num_splits) throws IOException {
		return VerticaUtil.getSplits(job, num_splits);
	}
}

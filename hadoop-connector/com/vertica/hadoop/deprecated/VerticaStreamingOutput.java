/* Copyright (c) 2005 - 2012 Vertica, an HP company -*- Java -*- */

package com.vertica.hadoop.deprecated;

import java.io.IOException;
import java.sql.Connection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import com.vertica.hadoop.VerticaConfiguration;
import com.vertica.hadoop.VerticaOutputFormat;

public class VerticaStreamingOutput implements OutputFormat<Text, Text> {
	private static final Log LOG = LogFactory.getLog("com.vertica.hadoop");

	@Override
	public void checkOutputSpecs(FileSystem filesystem, JobConf job)
			throws IOException {
		VerticaConfiguration vtconfig = new VerticaConfiguration(job);
		VerticaOutputFormat.checkOutputSpecs(vtconfig);
	}

	@Override
	public RecordWriter<Text, Text> getRecordWriter(FileSystem filesystem,
			JobConf job, String name, Progressable progress) throws IOException {
		VerticaConfiguration vtconfig = new VerticaConfiguration(job);

		try {
			Connection conn = vtconfig. getConnection(true);
			return new VerticaStreamingRecordWriter(conn, vtconfig);
		}
		catch (Exception e) { throw new IOException(e); }	
	}
}

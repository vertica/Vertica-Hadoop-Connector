package com.vertica.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.math.BigDecimal;

import java.sql.Types;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import com.vertica.hadoop.VerticaInputFormat;
import com.vertica.hadoop.VerticaOutputFormat;
import com.vertica.hadoop.VerticaRecord;

public class hdfs2vertica extends Configured implements Tool {

	public static class Map extends
		Mapper<LongWritable, Text, Text, VerticaRecord> {
		VerticaRecord record = null;
		Integer numColumns = null;
		String delimiter = null;
		String table = null;
		ArrayList<String> columns = null;

		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			try {
				record = new VerticaRecord(context.getConfiguration());
				numColumns = new Integer(record.size());

				Configuration conf = context.getConfiguration();

				String colNames = new String(conf.get("hdfs2vertica.columns"));
				StringTokenizer tok = new StringTokenizer(colNames, ",");
				if (tok.countTokens() != numColumns)
				{
					throw new IOException("The number of columns in the file (" + 
						tok.countTokens() + ") does not match the number of columns ("
						+ numColumns + ") in the table");
				}
			
				columns = new ArrayList<String>();
				for (int count = 0; count < numColumns; count++)
				{
					columns.add(tok.nextToken());
				}

				delimiter = new String(conf.get("hdfs2vertica.delimiter"));
				table = new String(conf.get("hdfs2vertica.table"));
			} catch (Exception e) {
				throw new IOException(e);
			}
		}
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();

			if (record == null) {
				throw new IOException("No output record found");
			}
			
			StringTokenizer tok = new StringTokenizer(line, delimiter);
			if (tok.countTokens() != numColumns)
			{
				throw new IOException("The number of columns in the file (" + 
						tok.countTokens() + ") does not match the number of columns ("
						+ numColumns + ") in row " + value.toString());
			}

			try {
				for (int count = 0; count < numColumns; count++)
				{
					record.setFromString(columns.get(count), tok.nextToken());
				}
			} catch (ParseException p) {
				throw new IOException(p.getMessage());
			}
			context.write(new Text(table), record);
		}
	}

	public Job getJob(String[] args) throws IOException {
		Configuration conf = getConf();
		Job job = new Job(conf);
    
		conf = job.getConfiguration();

		job.setJobName("HDFS To Vertica Transfer");
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VerticaRecord.class);
    
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(VerticaRecord.class);
	
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(VerticaOutputFormat.class);
    
		job.setJarByClass(hdfs2vertica.class);
		job.setMapperClass(Map.class);

		return job;
  	}

	@SuppressWarnings("serial")
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 4) {
			throw new IOException("Expect <file path> <table name> <delimiter> <file schema>");
		}
		Job job = getJob(args);
		Configuration conf = job.getConfiguration();
		conf.set("hdfs2vertica.table",args[1]);
		conf.set("hdfs2vertica.delimiter",args[2]);
		conf.set("hdfs2vertica.columns",args[3]);

		VerticaOutputFormat.setOutput(job, args[1]);
		FileInputFormat.setInputPaths(job, args[0]);
		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new hdfs2vertica(), args);
		System.exit(res);
	}
}

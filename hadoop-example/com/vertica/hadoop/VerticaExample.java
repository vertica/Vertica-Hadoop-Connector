package com.vertica.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;

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

import com.vertica.hadoop.VerticaConfiguration;
import com.vertica.hadoop.VerticaInputFormat;
import com.vertica.hadoop.VerticaOutputFormat;
import com.vertica.hadoop.VerticaRecord;

public class VerticaExample extends Configured implements Tool {

  public static class Map extends
      Mapper<LongWritable, VerticaRecord, Text, DoubleWritable> {

    public void map(LongWritable key, VerticaRecord value, Context context)
        throws IOException, InterruptedException {
	  if (value.get(4) != null && value.get(1) != null) {
	      context.write(new Text((String) value.get(4)), new DoubleWritable(
    	      (Long) value.get(1)));
	  }
    }
  }

  public static class Reduce extends
      Reducer<Text, DoubleWritable, Text, VerticaRecord> {
    VerticaRecord record = null;

    public void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
      try {
        record = new VerticaRecord(context.getConfiguration());
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
        throws IOException, InterruptedException {
      if (record == null) {
        throw new IOException("No output record found");
      }
	  record.set("a", 125);
      record.set("b", true);
      record.set("c", 'c');
      record.set("d", new java.sql.Date(Calendar.getInstance().getTimeInMillis()));
      record.set("f", 234.526);
      record.set("t", new java.sql.Timestamp(Calendar.getInstance().getTimeInMillis()));
      record.set("v", "foobar string");
      record.set("z", new byte[10]);
      context.write(new Text("mrtarget"), record);
    }
  }
 
  public Job getJob() throws IOException {
    Configuration conf = getConf();
    Job job = new Job(conf);
    
    conf = job.getConfiguration();
    conf.set("mapreduce.job.tracker", "local");

    job.setJobName("vertica test");

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(VerticaRecord.class);
    
	job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);
	
	job.setInputFormatClass(VerticaInputFormat.class);
    job.setOutputFormatClass(VerticaOutputFormat.class);
    
	job.setJarByClass(VerticaExample.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
 
    VerticaOutputFormat.setOutput(job, "mrtarget", true, "a int", "b boolean",
        "c char(1)", "d date", "f float", "t timestamp", "v varchar",
        "z varbinary");
    
    return job;
  }

  @SuppressWarnings("serial")
  @Override
  public int run(String[] args) throws Exception {
	Job job = getJob();
	
	VerticaOutputFormat.setOutput(job, "mrtarget", true, "a int", "b boolean",
        "c char(1)", "d date", "f float", "t timestamp", "v varchar",
        "z varbinary");

    VerticaInputFormat.setInput(job, "select * from allTypes where key = ?",
        "select distinct key from allTypes");
   
   
    job.waitForCompletion(true);

	job = getJob();
    Collection<List<Object>> params = new HashSet<List<Object>>() {
    };
    List<Object> param = new ArrayList<Object>();
    param.add(new Integer(0));
    params.add(param);
    VerticaInputFormat.setInput(job, "select * from allTypes where key = ?",
        params);
    job.waitForCompletion(true);

	job = getJob();
    VerticaInputFormat.setInput(job, "select * from allTypes where key = ?",
        "0", "1", "2");
    job.waitForCompletion(true);

	return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new VerticaExample(), args);
    System.exit(res);
  }
}

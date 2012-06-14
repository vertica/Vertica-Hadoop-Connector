
package com.vertica.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.sql.Date;
import java.sql.Timestamp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import com.vertica.hadoop.VerticaConfiguration;
import com.vertica.hadoop.VerticaInputFormat;
import com.vertica.hadoop.VerticaOutputFormat;
import com.vertica.hadoop.VerticaRecord;

public class TestExample extends VerticaTestCase implements Tool {

  public TestExample(String name) {
    super(name);
  }

  public static class Map extends
      Mapper<LongWritable, VerticaRecord, Text, DoubleWritable> {

    public void map(LongWritable key, VerticaRecord value, Context context)
        throws IOException, InterruptedException {
      context.write(new Text((String) value.get(1)), new DoubleWritable(
          (Long) value.get(0)));
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

    protected void reduce(Text key, Iterable<DoubleWritable> values,
        Context context) throws IOException, InterruptedException {
      if (record == null) {
        throw new IOException("No output record found");
      }
      
      record.set(0, 125);
      record.set(1, true);
      record.set(2, 'c');
      record.set(3, new java.sql.Date(Calendar.getInstance().getTimeInMillis()));
      record.set(4, 234.526);
      record.set(5, new java.sql.Timestamp(Calendar.getInstance().getTimeInMillis()));
      record.set(6, "foobar string");
      record.set(7, new byte[10]);
      context.write(new Text("mrtarget"), record);
    }
  }

  public Job getJob() throws IOException {
    Configuration conf = new Configuration(true);
    Job job = new Job(conf);
    
    conf = job.getConfiguration();
    conf.set("mapreduce.job.tracker", "local");

    job.setJarByClass(TestExample.class);
    job.setJobName("vertica test");

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(VerticaRecord.class);
    job.setInputFormatClass(VerticaInputFormat.class);
    job.setOutputFormatClass(VerticaOutputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    VerticaOutputFormat.setOutput(job, "mrtarget", true, "a int", "b boolean",
        "c char(1)", "d date", "f float", "t timestamp", "v varchar",
        "z varbinary");
    VerticaConfiguration.configureVertica(conf,
        new String[] { AllTests.getHostname() }, AllTests.getDatabase(),
        AllTests.getPort(), AllTests.getUsername(), AllTests.getPassword());
    return job;
  }

  @SuppressWarnings("serial")
  public void testExample() throws Exception {
    if(!AllTests.isSetup()) {
      return;
    }

    Job job = getJob();
    VerticaInputFormat.setInput(job, "select key, varcharcol from allTypes");
    job.waitForCompletion(true);

    job = getJob();
    VerticaInputFormat.setInput(job, "select key,varcharcol from allTypes where key = ?",
        "select distinct key from allTypes");
    job.waitForCompletion(true);

    job = getJob();
    Collection<List<Object>> params = new HashSet<List<Object>>() {
    };
    List<Object> param = new ArrayList<Object>();
    param.add(new Integer(0));
    params.add(param);
    VerticaInputFormat.setInput(job, "select key,varcharcol from allTypes where key = ?",
        params);
    job.waitForCompletion(true);

    job = getJob();
    VerticaInputFormat.setInput(job, "select key,varcharcol from allTypes where key = ?",
        "0", "1", "2");
    job.waitForCompletion(true);
   
    VerticaOutputFormat.optimize(job.getConfiguration());
  }

  @Override
  public int run(String[] arg0) throws Exception {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Configuration getConf() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setConf(Configuration arg0) {
    // TODO Auto-generated method stub

  }
}

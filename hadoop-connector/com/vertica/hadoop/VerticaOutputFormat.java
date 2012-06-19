package com.vertica.hadoop;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.DatabaseMetaData;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Output formatter for loading data to Vertica
 * 
 */
public class VerticaOutputFormat extends OutputFormat<Text, VerticaRecord> {
	/**
	  * Set the output table
	  * 
	  * @param conf
	  * @param tableName
	  */
	public static void setOutput(Job job, String tableName) {
		setOutput(job, tableName, false);
	}

	/**
	  * Set the output table and whether to truncate it before loading
	  * 
	  * @param job
	  * @param tableName
	  * @param truncateTable
	  */
	public static void setOutput(Job job, String tableName, boolean truncateTable) {
		setOutput(job, tableName, truncateTable, (String[])null);
	}

	/**
	  * Set the output table, whether to truncate it before loading if it already exists or create
	  * table specification if it doesn't exist with column definitions
	  * 
	  * @param job
	  * @param tableName
	  * @param truncateTable
	  * @param tableDef
	  *          list of column definitions such as "foo int", "bar varchar(10)"
	  */
	public static void setOutput(Job job, String tableName, 
			boolean truncateTable, String... tableDef) {
		VerticaConfiguration vtconfig = new VerticaConfiguration(
				job.getConfiguration());
		vtconfig.setOutputTableName(tableName);
		vtconfig.setOutputTableDef(tableDef);
		//the following performs truncate table and not drop table
		vtconfig.setDropTable(truncateTable);
  	}

	/** {@inheritDoc} */
	public void checkOutputSpecs(JobContext context) throws IOException {
		checkOutputSpecs(new VerticaConfiguration(context.getConfiguration()));
	}

	public static void checkOutputSpecs(VerticaConfiguration vtconfig) throws IOException {
		Relation vTable = new Relation(vtconfig.getOutputTableName());
		if (vTable.isNull())
		  throw new IOException("Vertica output requires a table name defined by "
			  + VerticaConfiguration.OUTPUT_TABLE_NAME_PROP);
		String[] def = vtconfig.getOutputTableDef();
		boolean dropTable = vtconfig.getDropTable();

		Statement stmt = null;
		try {
			Connection conn = vtconfig.getConnection(true);
			DatabaseMetaData dbmd = conn.getMetaData();
			ResultSet rs = dbmd.getTables(null, vTable.getSchema(), vTable.getTable(), null);
			boolean tableExists = rs.next();

			stmt = conn.createStatement();

			if (tableExists && dropTable) {
				stmt = conn.createStatement();
				stmt.execute("TRUNCATE TABLE " + vTable.getQualifiedName().toString());
			}

			// create table if it doesn't exist
			if (!tableExists) {
				if (def == null)
					throw new RuntimeException("Table " + vTable.getQualifiedName().toString()
							+ " does not exist and no table definition provided");
				if (!vTable.isDefaultSchema()) {
					stmt.execute("CREATE SCHEMA IF NOT EXISTS " + vTable.getSchema());
				}
				StringBuffer tabledef = new StringBuffer("CREATE TABLE ").append(
						vTable.getQualifiedName().toString()).append(" (");
				for (String column : def)
					tabledef.append(column).append(",");
				tabledef.replace(tabledef.length() - 1, tabledef.length(), ")");
				stmt.execute(tabledef.toString());
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			if (stmt != null)
				try {
					stmt.close();
				} catch (SQLException e) {
					throw new RuntimeException(e);
			}
		}
	}

  	/** {@inheritDoc} */
	public RecordWriter<Text, VerticaRecord> getRecordWriter(
			TaskAttemptContext context) throws IOException {

		VerticaConfiguration config = new VerticaConfiguration(
				context.getConfiguration());

		String name = context.getJobName();
		String table = config.getOutputTableName();
		try {
			Connection conn = config.getConnection(true);
			return new VerticaRecordWriter(conn, table, config.getBatchSize());
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e.getMessage());
		}
	}

	/**
	  * Optionally called at the end of a job to optimize any newly created and
	  * loaded tables. Useful for new tables with more than 100k records.
	  * 
	  * @param conf
	  * @throws Exception
	  */
	public static void optimize(Configuration conf) throws Exception {
		VerticaConfiguration vtconfig = new VerticaConfiguration(conf);
		Connection conn = vtconfig.getConnection(true);

		// TODO: consider more tables and skip tables with non-temp projections 
		Relation vTable = new Relation(vtconfig.getOutputTableName());
		Statement stmt = conn.createStatement();
		ResultSet rs = null;
	    HashSet<String> tablesWithTemp = new HashSet<String>();

	    //for now just add the single output table
	    tablesWithTemp.add(vTable.getQualifiedName().toString());

	    // map from table name to set of projection names
	    HashMap<String, Collection<String>> tableProj = new HashMap<String, Collection<String>>();
    	rs = stmt.executeQuery("select projection_schema, anchor_table_name, projection_name from projections;");
	    while(rs.next()) {
    		String ptable = rs.getString(1) + "." + rs.getString(2);
			if(!tableProj.containsKey(ptable)) {
				tableProj.put(ptable, new HashSet<String>());
			}

			tableProj.get(ptable).add(rs.getString(3));
		}
    
		for(String table : tablesWithTemp) {
			if(!tableProj.containsKey(table)) {
				throw new RuntimeException("Cannot optimize table with no data: " + table);
			}
		}
    
	    String designName = (new Integer(conn.hashCode())).toString();
    	stmt.execute("select dbd_create_workspace('" + designName + "')");
	    stmt.execute("select dbd_create_design('" + designName + "', '"
    	    + designName + "')");
	    stmt.execute("select dbd_add_design_tables('" + designName + "', '"
    	    + vTable.getQualifiedName().toString() + "')");
	    stmt.execute("select dbd_populate_design('" + designName + "', '"
    	    + designName + "')");

		//Execute
	    stmt.execute("select dbd_create_deployment('" + designName + "', '" + designName + "')");
    	stmt.execute("select dbd_add_deployment_design('" + designName + "', '" + designName + "', '" + designName + "')");
	    stmt.execute("select dbd_add_deployment_drop('" + designName + "', '" + designName + "')");
    	stmt.execute("select dbd_execute_deployment('" + designName + "', '" + designName + "')");

		//Cleanup
    	stmt.execute("select dbd_drop_deployment('" + designName + "', '" + designName + "')");
	    stmt.execute("select dbd_remove_design('" + designName + "', '" + designName + "')");
	    stmt.execute("select dbd_drop_design('" + designName + "', '" + designName + "')");
	    stmt.execute("select dbd_drop_workspace('" + designName + "')");
	}

	/** (@inheritDoc) */
	public OutputCommitter getOutputCommitter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new FileOutputCommitter(FileOutputFormat.getOutputPath(context),
				context);
	}
}

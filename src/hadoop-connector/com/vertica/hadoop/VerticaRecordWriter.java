package com.vertica.hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Vector;
import java.util.Map;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class VerticaRecordWriter extends RecordWriter<Text, VerticaRecord> {
  	private static final Log LOG = LogFactory.getLog("com.vertica.hadoop");
	
	Relation vTable = null;
	String schemaName = null;
	Connection connection = null;
	PreparedStatement statement = null;
	long batchSize = 0;
	long numRecords = 0;

	public VerticaRecordWriter(Connection conn, String writerTable, long batch) 
		throws SQLException 
	{
		this.connection = conn;
		batchSize = batch;
    
		vTable = new Relation(writerTable);
 
		StringBuilder sb = new StringBuilder();
		sb.append("INSERT INTO ");
		sb.append(vTable.getQualifiedName());

		StringBuilder values = new StringBuilder();
		values.append(" VALUES(");
		sb.append("(");

		String metaStmt = "select ordinal_position, column_name, data_type, is_identity, data_type_name " +
			"from v_catalog.odbc_columns " + 
			"where schema_name = ? and table_name = ? "
			+ "order by ordinal_position;";

		PreparedStatement stmt = conn.prepareStatement(metaStmt);
		stmt.setString(1, vTable.getSchema());
		stmt.setString(2, vTable.getTable());

		ResultSet rs = stmt.executeQuery();
		boolean addComma = false;
		while (rs.next()) {
			if (!rs.getBoolean(4)) {
				if (addComma) {
					sb.append(',');
					values.append(',');
				}
				sb.append(rs.getString(2));
				values.append('?');
				addComma = true;
			} else {
				LOG.debug("Skipping identity column " + rs.getString(4));
			}
		}

		sb.append(')');
		values.append(')');
		sb.append(values.toString());

		statement = conn.prepareStatement(sb.toString());
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException {
		try {
			statement.executeBatch();
			connection.close();
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public void write(Text table, VerticaRecord record) throws IOException {
		if (table != null && !table.toString().equals(vTable.getTable()))
			throw new IOException("Writing to different table " + table.toString()
					+ ". Expecting " + vTable.getTable());

		try {
			record.write(statement);
			numRecords++;
			if (numRecords % batchSize == 0) {
				statement.executeBatch();
			}
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
}

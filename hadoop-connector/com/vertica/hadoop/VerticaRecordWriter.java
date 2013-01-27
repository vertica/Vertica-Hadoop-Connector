/*
Copyright (c) 2005 - 2012 Vertica, an HP company -*- Java -*-
Copyright 2013, Twitter, Inc.


Licensed under the Apache License, Version 2.0 (the "License");

you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,

WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.vertica.hadoop;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
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
      // committing and closing the connection is handled by the VerticaTaskOutputCommitter
      if (LOG.isDebugEnabled()) { LOG.debug("executeBatch called during close"); }
			statement.executeBatch();
  } catch (SQLException e) {
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
        if (LOG.isDebugEnabled()) { LOG.debug("executeBatch called on batch of size " + batchSize); }
				statement.executeBatch();
			}
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
}

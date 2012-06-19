/* Copyright (c) 2005 - 2012 Vertica, an HP company -*- Java -*- */

package com.vertica.hadoop.deprecated;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.text.ParseException;
import java.util.Vector;
import java.util.Map;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import com.vertica.hadoop.VerticaRecord;
import com.vertica.hadoop.Relation;
import com.vertica.hadoop.VerticaConfiguration;

public class VerticaStreamingRecordWriter implements RecordWriter<Text, Text> {
	private static final Log LOG = LogFactory.getLog("com.vertica.hadoop");

	Relation vTable = null; 
	Connection connection = null;
	PreparedStatement statement = null;
	long batchSize = 0;
	long numRecords = 0;
	String delimiter = VerticaConfiguration.DELIMITER;
	String terminator = VerticaConfiguration.RECORD_TERMINATOR;
	VerticaRecord record = null;

	public VerticaStreamingRecordWriter(Connection conn, 
			VerticaConfiguration vtconfig) 
				throws IOException, SQLException, ClassNotFoundException {
		connection = conn;
		batchSize = vtconfig.getBatchSize();
		delimiter = Pattern.quote(vtconfig.getOutputDelimiter());
		terminator = vtconfig.getOutputRecordTerminator();
		
		vTable = new Relation(vtconfig.getOutputTableName());
 		
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
		record = new VerticaRecord(vtconfig.getConfiguration());
	}
	
	@Override
	public void close(Reporter arg0) throws IOException {
		try {
			statement.executeBatch();
			connection.close();
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public void write(Text table, Text row) throws IOException {
		try {
			String line = row.toString();
			line = line.replaceAll(terminator + "$", "");
			if (line.length() == 0) return;

			String [] tokens = line.split(delimiter);
			
			for(int count = 0; count < tokens.length; count++) {
				record.setFromString(count, tokens[count]);
			}
			record.write(statement);
			numRecords++;
			if (numRecords % batchSize == 0) {
				statement.executeBatch();
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IOException(e);
		} catch (ParseException e) {
			e.printStackTrace();
			throw new IOException(e);
		}
	}
}

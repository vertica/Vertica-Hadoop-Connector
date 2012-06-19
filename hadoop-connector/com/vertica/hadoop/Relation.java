/* Copyright (c) 2005 - 2012 Vertica, an HP company -*- Java -*- */
package com.vertica.hadoop;

public class Relation {
	private String table = null;
	private String schema = null;
	private String database = null;
	private boolean defSchema = false;

	public Relation(String name) {
		if (name == null) return;

		String[] splut = name.split("\\.");

		if (splut.length == 3) {
			database = splut[0];
			schema = splut[1];
			table = splut[2];
		} else if (splut.length == 2) {
			schema = splut[0];
			table = splut[1];
		} else if (splut.length == 1) {
			defSchema = true;
			schema = "public";
			table = splut[0];
		}
	}

	public boolean isNull() {
		return table == null;
	}

	public boolean isDefaultSchema() {
		return defSchema;
	}

	public String getDatabase() {
		return database;
	}

	public String getSchema() {
		return schema;
	}

	public String getTable() {
		return table;
	}

	public StringBuilder getQualifiedName() {
		StringBuilder sb = new StringBuilder();
		if (database != null) {
			sb.append(database);
			sb.append('.');
		}

		sb.append(schema);
		sb.append('.');
		sb.append(table);

		return sb;
	}
}

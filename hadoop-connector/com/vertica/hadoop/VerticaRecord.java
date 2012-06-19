/* Copyright (c) 2005 - 2012 Vertica, an HP company -*- Java -*- */

package com.vertica.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Vector;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

//import com.vertica.jdbc.VerticaDayTimeInterval;
//import com.vertica.jdbc.VerticaYearMonthInterval;

/**
 * Encapsulates a record read from or to be written to Vertica.
 * 
 * A record consists of a list of columns in a specified order.
 * The list of columns is automatically determined by:
 * <ol>
 * <li>ResultSet of <i>select</i> query</li>
 * <li>Columns in the table</li>
 * </ol>
 *
 * The record also contains many helper functions to serialize/deserialize data
 * into various formats.
 * 
 */
public class VerticaRecord implements Writable {
  	private static final Log LOG = LogFactory.getLog("com.vertica.hadoop");
	/**
	 * No. of columns in a record.
	 */ 
	int columns = 0;
	
	/**
	 * An ordered list of names of each column.
	 * There is an entry for each column. However, there is 
	 * no guarantee that all columns have a name.
	 */
	Vector<String> names = null;
	
	/**
	 * An ordered list of types of each column
	 */ 
	Vector<Integer> types = null;
	/**
	 * An ordered list of values of each column
	 */ 
	Vector<Object> values = null;
	/**
	 * A map to easily get the position of a column from the name.
	 */
	HashMap<String, Integer> nameMap = null;

	DateFormat sqlfmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	/**
	 * Default constructor should NOT be invoked by customers.
	 * Available mainly for Hadoop framework.
	 */ 
	public VerticaRecord() {
		names = new Vector<String>();
		types = new Vector<Integer>();
		values = new Vector<Object>();
		nameMap = new HashMap<String, Integer>();
	}

	/**
	 * Constructor used when data is transferred to Vertica.
	 * All values in <code>conf</code> is validated.
	 *
	 * @param conf Provides all necessary information for setting up the record.
	 *
	 * @throws SQLException JDBC driver or Vertica encounters an error.
	 */

	public VerticaRecord(Configuration conf) 
		throws ClassNotFoundException, SQLException, IOException {
		VerticaConfiguration config = new VerticaConfiguration(conf);
		String outTable = config.getOutputTableName();
		Connection conn = config.getConnection(true);

		DatabaseMetaData dbmd = conn.getMetaData();
		Relation vTable = new Relation(outTable);
		
		names = new Vector<String>();
		types = new Vector<Integer>();
		values = new Vector<Object>();
		nameMap = new HashMap<String, Integer>();
		HashMap <String, Integer> typeMap = new HashMap<String, Integer>();

		String metaStmt = "select ordinal_position, column_name, data_type, is_identity, data_type_name " +
			"from v_catalog.odbc_columns " + 
			"where schema_name = ? and table_name = ? "
			+ "order by ordinal_position;";

		PreparedStatement stmt = conn.prepareStatement(metaStmt);
		stmt.setString(1, vTable.getSchema());
		stmt.setString(2, vTable.getTable());

		ResultSet rs = stmt.executeQuery();
		boolean seenIdentity = false;
		while (rs.next()) {
			if (!rs.getBoolean(4)) {
				columns++;
				int index = rs.getInt(1) - 1;
				if (seenIdentity) {
					index--;
				}
				nameMap.put(rs.getString(2), index);
				typeMap.put(rs.getString(2), rs.getInt(3));
				LOG.debug(columns + ") " + "[" + rs.getInt(1) + "] " + rs.getString(2) + ":" + rs.getInt(3) + ":" + 
					rs.getString(5));
			} else {
				LOG.debug("Skipping identity column " + rs.getString(4));
				seenIdentity = true;
			}
		}

		names.setSize(nameMap.size());
		types.setSize(nameMap.size());
		values.setSize(nameMap.size());

		for (Map.Entry<String, Integer> entry : nameMap.entrySet()) {
			String key = entry.getKey();
			Integer value = entry.getValue();
			
			names.set(value.intValue(), key);
			types.set(value.intValue(), typeMap.get(key));
		}
	}

	/**
	 * Create a new VerticaRecord class out of a query result set
	 * @param results
	 *          ResultSet returned from running input split query
	 * @throws SQLException
	*/
	public VerticaRecord(ResultSet results) throws SQLException {
		ResultSetMetaData meta = results.getMetaData();
		columns = meta.getColumnCount();
		names = new Vector<String>(columns + 1);
		types = new Vector<Integer>(columns + 1);
		values = new Vector<Object>(columns + 1);
		nameMap = new HashMap<String, Integer>();

		for (int i = 1; i <= columns; i++) {
			LOG.debug("Inserting " + meta.getCatalogName(i) + ":" + meta.getColumnType(i) + ":" + meta.getColumnTypeName(i) + " at " + i);
			names.add(meta.getCatalogName(i));
			nameMap.put(meta.getCatalogName(i),i-1);
//			if (meta.getColumnType(i) == 1111)
//				types.add(VerticaDayTimeInterval.INTERVAL_DAY_TO_SECOND);
//			else
				types.add(meta.getColumnType(i));
			values.add(null);
		}
	}

	/**
	 * Gets the number of columns in the record.
	 * @return
	 */ 
	public int size() {
		return names.size();
	}

	/**
	 * Gets the object for a column.
	 * @param name Name of the column.
	 * @return Object containing the value of the column.
	 */
	public Object get(String name) throws IOException {
		if (names == null || names.size() == 0)
			throw new IOException("Cannot set record by name if names not initialized");
		if (!nameMap.containsKey(name)) 
			throw new IOException("Column \"" + name + "\" does not exist");
		return values.get(nameMap.get(name));
	}
	
	/**
	 * Gets the object for a column.
	 * @param pos Ordinal position of the column.
	 * @return Object containing the value of the column.
	 */
	public Object get(int pos) throws IOException, IndexOutOfBoundsException{
		if (pos >= names.size())
			throw new IndexOutOfBoundsException("Index " + pos
					+ " greater than input size " + values.size());
		return values.get(pos);
	}

	/**
	 * Gets the type for a column.
	 * @param name Name of the column.
	 * @return <code>java.sql.types</code> enum in an int.
	 */
	public int getType(String name) throws IOException {
		if (names == null || names.size() == 0)
			throw new IOException("Cannot set record by name if names not initialized");
		if (!nameMap.containsKey(name)) 
			throw new IOException("Column \"" + name + "\" does not exist");
		return types.get(nameMap.get(name));
	}

	/**
	 * Gets the type for a column.
	 * @param pos Ordinal position of the column.
	 * @return <code>java.sql.types</code> enum in an int.
	 */
	public int getType(int pos) {
		if (pos >= names.size())
			throw new IndexOutOfBoundsException("Index " + pos
					+ " greater than input size " + values.size());
		return types.get(pos);
	}

	/**
	 * Gets the ordinal position of a column.
	 * @param name Name of the column.
	 * @return int containing the position.
	 */
	public int getOrdinalPosition(String name) throws IOException {
		if (names == null || names.size() == 0)
			throw new IOException("Cannot set record by name if names not initialized");
		if (!nameMap.containsKey(name)) 
			throw new IOException("Column \"" + name + "\" does not exist");
		return nameMap.get(name);
	}

	public void add(Object value) {
		columns++;
		names.add(null);
		types.add(getType(value));
		values.add(value);
	}

	public void set(String name, Object value) throws IOException {
		if (names == null || names.size() == 0)
			throw new IOException("Cannot set record by name if names not initialized");
		if (!nameMap.containsKey(name)) 
			throw new IOException("Column \"" + name + "\" does not exist");
		set(nameMap.get(name), value);
	}

	/**
	  * set a value, 0 indexed
	  * 
	  * @param i
	  */
	public void set(int i, Object value) throws IOException{
		if (i >= names.size())
			throw new IndexOutOfBoundsException("Index " + i
					+ " greater than input size " + values.size());
		values.set(i, value);
	}

	public void setFromString(String name, String s) 
		throws IOException, ParseException 
	{
		if (names == null || names.size() == 0)
			throw new IOException("Cannot set record by name if names not initialized");
		if (!nameMap.containsKey(name)) 
			throw new IOException("Column \"" + name + "\" does not exist");
		setFromString(nameMap.get(name), s);
	}


	public void setFromString(Integer index, String s) 
		throws IOException, ParseException
	{
		if (index >= names.size())
			throw new IndexOutOfBoundsException("Index " + index
					+ " greater than input size " + values.size());
		Integer type = types.get(index);

		if (s.length() == 0)
		{
			set(index, null);
			return;
		}

		// switch statement uses fall through to handle type variations
		// e.g. type specified as BIGINT but passed in as Integer
		switch (type) {
			case Types.NULL:
				set(index, null);
				break;
			case Types.BIGINT:
			case Types.INTEGER:
			case Types.TINYINT:
			case Types.SMALLINT:
			{
				set(index, s);
				break;
			}
			case Types.REAL:
			case Types.DECIMAL:
			case Types.NUMERIC:
			{
				set(index, s);
				break;
			}
			case Types.DOUBLE:
			case Types.FLOAT:
			{
				set(index, s);
				break;
			}
			case Types.BINARY:
			case Types.LONGVARBINARY:
			case Types.VARBINARY:
			{
				int len = s.length();
				byte[] data = new byte[(len  + 1)/ 2];
				int dstIndex = 0;
				int srcIndex = 0;
				if (len % 2 == 1) {
					data[dstIndex] = (byte)(Character.digit(s.charAt(srcIndex), 16));
					dstIndex++;
					srcIndex++;
				}
			
				for (; srcIndex < len; srcIndex += 2, dstIndex++) {
					data[dstIndex] = (byte) ((Character.digit(s.charAt(srcIndex), 16) << 4)
							+ Character.digit(s.charAt(srcIndex+1), 16));
				}
				set(index, data);
				break;
			}
			case Types.BIT:
			case Types.BOOLEAN:
			{
				set(index, s);
				break;
			}
			case Types.LONGNVARCHAR:
			case Types.LONGVARCHAR:
			case Types.NCHAR:
			case Types.NVARCHAR:
			case Types.VARCHAR:
			case Types.CHAR:
			{
				set(index, s);
				break;
			}
			case Types.DATE:
			case Types.TIME:
			case Types.TIMESTAMP:
/*			case VerticaDayTimeInterval.INTERVAL_DAY:
			case VerticaDayTimeInterval.INTERVAL_DAY_TO_HOUR:
			case VerticaDayTimeInterval.INTERVAL_DAY_TO_MINUTE:
			case VerticaDayTimeInterval.INTERVAL_DAY_TO_SECOND:
			case VerticaDayTimeInterval.INTERVAL_HOUR:
			case VerticaDayTimeInterval.INTERVAL_HOUR_TO_MINUTE:
			case VerticaDayTimeInterval.INTERVAL_HOUR_TO_SECOND:
			case VerticaDayTimeInterval.INTERVAL_MINUTE:
			case VerticaDayTimeInterval.INTERVAL_MINUTE_TO_SECOND:
			case VerticaDayTimeInterval.INTERVAL_SECOND:
			case VerticaYearMonthInterval.INTERVAL_YEAR:
			case VerticaYearMonthInterval.INTERVAL_YEAR_TO_MONTH:
			case VerticaYearMonthInterval.INTERVAL_MONTH:*/
			default:
			{
				set(index, s);
				break;
			}
		}
	}

	public String toString(int index, ResultSet results)
		throws SQLException {
		int oneBasedIndex = index + 1;
		int type = types.get(index).intValue();
		// switch statement uses fall through to handle type variations
		// e.g. type specified as BIGINT but passed in as Integer
		if (type == Types.NULL || results.getObject(oneBasedIndex) == null) {
			return "";
		} else if (type == Types.BIGINT) {
			Long l = new Long(results.getLong(oneBasedIndex));
			return l.toString();
		} else if (type == Types.DOUBLE) {
			Double d = new Double(results.getDouble(oneBasedIndex));
			return d.toString();
		} else if (type == Types.NUMERIC) {
			return results.getBigDecimal(oneBasedIndex).toString();
		} else if (type == Types.BINARY
				|| type == Types.LONGVARBINARY
				|| type == Types.VARBINARY) {
			byte [] barr = results.getBytes(oneBasedIndex);
			return new String(barr, 0, barr.length);
		} else if (type == Types.BOOLEAN || 
				type == Types.BIT) {
			if (results.getBoolean(oneBasedIndex))
				return "true";
			else
				return "false";
		}
		else if (type == Types.VARCHAR
				|| type == Types.CHAR) {
			return results.getString(oneBasedIndex);
		} else if (type == Types.DATE) {
			return results.getDate(oneBasedIndex).toString();
		} else if (type == Types.TIME) {
			return results.getTime(oneBasedIndex).toString();
		} else if (type == Types.TIMESTAMP) {
			return results.getTimestamp(oneBasedIndex).toString();
		}
/*		else if (type == VerticaDayTimeInterval.INTERVAL_DAY
				|| type == VerticaDayTimeInterval.INTERVAL_DAY_TO_HOUR
				|| type == VerticaDayTimeInterval.INTERVAL_DAY_TO_MINUTE
				|| type == VerticaDayTimeInterval.INTERVAL_HOUR
				|| type == VerticaDayTimeInterval.INTERVAL_HOUR_TO_MINUTE
				|| type == VerticaDayTimeInterval.INTERVAL_HOUR_TO_SECOND
				|| type == VerticaDayTimeInterval.INTERVAL_MINUTE
				|| type == VerticaDayTimeInterval.INTERVAL_MINUTE_TO_SECOND
				|| type == VerticaDayTimeInterval.INTERVAL_SECOND) {
			return ((VerticaDayTimeInterval) results.getObject(oneBasedIndex)).toString();
		} else if (type == VerticaDayTimeInterval.INTERVAL_DAY_TO_SECOND) {
			VerticaDayTimeInterval v = (VerticaDayTimeInterval) results.getObject(index);
			StringBuffer sb = new StringBuffer();
			if (v.getDay() == 0)
				sb.append("0 ");
			sb.append(v.toString());
			return sb.toString();
		} else if (type == VerticaYearMonthInterval.INTERVAL_YEAR
				|| type == VerticaYearMonthInterval.INTERVAL_YEAR_TO_MONTH
				|| type == VerticaYearMonthInterval.INTERVAL_MONTH) {
			return ((VerticaYearMonthInterval) results.getObject(oneBasedIndex)).toString();
		}*/
		else
			return results.getObject(oneBasedIndex).toString();
	}

	public static int getType(Object obj) {
		if (obj == null) {
			return Types.NULL;
		} else if (obj instanceof Long) {
			return Types.BIGINT;
		} else if (obj instanceof LongWritable) {
			return Types.BIGINT;
		} else if (obj instanceof VLongWritable) {
			return Types.BIGINT;
		} else if (obj instanceof VIntWritable) {
			return Types.INTEGER;
		} else if (obj instanceof Integer) {
			return Types.INTEGER;
		} else if (obj instanceof Short) {
			return Types.SMALLINT;
		} else if (obj instanceof BigDecimal) {
			return Types.NUMERIC;
		} else if (obj instanceof DoubleWritable) {
			return Types.DOUBLE;
		} else if (obj instanceof Double) {
			return Types.DOUBLE;
		} else if (obj instanceof Float) {
			return Types.FLOAT;
		} else if (obj instanceof FloatWritable) {
			return Types.FLOAT;
		} else if (obj instanceof byte[]) {
			return Types.BINARY;
		} else if (obj instanceof ByteWritable) {
			return Types.BINARY;
		} else if (obj instanceof Boolean) {
			return Types.BOOLEAN;
		} else if (obj instanceof BooleanWritable) {
			return Types.BOOLEAN;
		} else if (obj instanceof Character) {
			return Types.CHAR;
		} else if (obj instanceof String) {
			return Types.VARCHAR;
		} else if (obj instanceof BytesWritable) {
			return Types.VARCHAR;
		} else if (obj instanceof Text) {
			return Types.VARCHAR;
		} else if (obj instanceof java.util.Date) {
			return Types.DATE;
		} else if (obj instanceof Date) {
			return Types.DATE;
		} else if (obj instanceof Time) {
			return Types.TIME;
		} else if (obj instanceof Timestamp) {
			return Types.TIMESTAMP;
		} else {
			throw new RuntimeException("Unknown type " + obj.getClass().getName()
					 + " passed to Vertica Record");
		}
	}
	
	public static Object readField(int type, DataInput in) throws IOException {
		if (type == Types.NULL)			
			return null;
		else if (type == Types.BIGINT)	
			return in.readLong();
		else if (type == Types.INTEGER)	
			return in.readInt();
		else if (type == Types.TINYINT
				|| type == Types.SMALLINT)
			return in.readShort();
		else if (type == Types.REAL 
				|| type == Types.DECIMAL
				|| type == Types.NUMERIC)
			return new BigDecimal(Text.readString(in));
		else if (type == Types.DOUBLE)
			return in.readDouble();
		else if (type == Types.FLOAT)
			return in.readFloat();
		else if (type == Types.BINARY
				|| type == Types.LONGVARBINARY
				|| type == Types.VARBINARY)
			return StringUtils.hexStringToByte(Text.readString(in));
		else if (type == Types.BIT
				|| type == Types.BOOLEAN)
			return in.readBoolean();
		else if (type == Types.CHAR)
			return in.readChar();
		else if (type == Types.LONGNVARCHAR
				|| type == Types.LONGVARCHAR
				|| type == Types.NCHAR
				|| type == Types.NVARCHAR
				|| type == Types.VARCHAR)
			return Text.readString(in);
		else if (type == Types.DATE)
			return new Date(in.readLong());
		else if (type == Types.TIME)
			return new Time(in.readLong());
		else if (type == Types.TIMESTAMP)
			return new Timestamp(in.readLong());
		else
			throw new IOException("Unknown type value " + type);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		columns = in.readInt();
		if (types.size() > 0)
			types.clear();
		for (int i = 0; i < columns; i++)
			types.set(i, in.readInt());
		for (int i = 0; i < columns; i++) {
			int type = types.get(i);
			values.set(i, readField(type, in));
		}
	}

	public static void write(Object obj, int type, DataOutput out) throws IOException {
		if(obj == null) return;
		switch (type) {
			case Types.BIGINT:
				out.writeLong((Long) obj);
				break;
			case Types.INTEGER:
				out.writeInt((Integer) obj);
				break;
			case Types.TINYINT:
			case Types.SMALLINT:
				out.writeShort((Short) obj);
				break;
			case Types.REAL:
			case Types.DECIMAL:
			case Types.NUMERIC:
				Text.writeString(out, obj.toString());
				break;
			case Types.DOUBLE:
				out.writeDouble((Double) obj);
				break;
			case Types.FLOAT:
				out.writeFloat((Float) obj);
				break;
			case Types.BINARY:
			case Types.LONGVARBINARY:
			case Types.VARBINARY:
				Text.writeString(out, StringUtils.byteToHexString((byte[]) obj));
				break;
			case Types.BIT:
			case Types.BOOLEAN:
				out.writeBoolean((Boolean) obj);
				break;
			case Types.CHAR:
				out.writeChar((Character) obj);
				break;
			case Types.LONGNVARCHAR:
			case Types.LONGVARCHAR:
			case Types.NCHAR:
			case Types.NVARCHAR:
			case Types.VARCHAR:
				Text.writeString(out, (String) obj);
				break;
			case Types.DATE:
				if (obj instanceof java.util.Date) {
					out.writeLong(((java.util.Date) obj).getTime());
				} else {
					out.writeLong(((Date) obj).getTime());
				}
				break;
			case Types.TIME:
				out.writeLong(((Time) obj).getTime());
				break;
			case Types.TIMESTAMP:
				out.writeLong(((Timestamp) obj).getTime());
				break;
			default:
				throw new IOException("Unknown type value " + type);
		}
	}
  
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(columns);

		for (int i = 0; i < columns; i++) {
			Object obj = values.get(i);
			Integer type = types.get(i);
			if(obj == null) 
				out.writeInt(Types.NULL);
			else 
				out.writeInt(type);
		}

		for (int i = 0; i < columns; i++) {
			Object obj = values.get(i);
			Integer type = types.get(i);

			if(obj == null) continue;
			write(obj, type, out);
		}
	}

	public void write(PreparedStatement stmt) throws SQLException {
		for (int i = 0; i < columns; i++) {
			int oneBasedIndex = i + 1;
			Object obj = values.get(i);
			Integer type = types.get(i);

			if (obj == null) {
				stmt.setObject(oneBasedIndex, obj);
				continue;
			}

			if (obj instanceof java.lang.String) {
				stmt.setString(oneBasedIndex, (String) obj);
				continue;
			}
			
			switch (type) {
				case Types.BIGINT:
				case Types.INTEGER:
				case Types.TINYINT:
				case Types.SMALLINT:
					stmt.setLong(oneBasedIndex, ((Number) obj).longValue());
					break;
				case Types.NUMERIC:
					stmt.setBigDecimal(oneBasedIndex, (BigDecimal)obj);
					break;
				case Types.FLOAT:
					stmt.setFloat(oneBasedIndex, (Float) obj);
					break;
				case Types.DOUBLE:
					stmt.setDouble(oneBasedIndex, (Double) obj);
					break;
				case Types.BINARY:
				case Types.LONGVARBINARY:
				case Types.VARBINARY:
					stmt.setBytes(oneBasedIndex,(byte[]) obj);
					break;
				case Types.BIT:
				case Types.BOOLEAN:
					stmt.setBoolean(oneBasedIndex, (Boolean) obj);
					break;
				case Types.CHAR:
					stmt.setString(oneBasedIndex, ((Character) obj).toString());
					break;
				case Types.LONGNVARCHAR:
				case Types.LONGVARCHAR:
				case Types.NCHAR:
				case Types.NVARCHAR:
				case Types.VARCHAR:
					stmt.setString(oneBasedIndex, (String) obj);
					break;
				case Types.DATE:
					stmt.setDate(oneBasedIndex, (Date) obj);
					break;
				case Types.TIME:
					stmt.setTime(oneBasedIndex, (Time) obj);
					break;
				case Types.TIMESTAMP:
					stmt.setTimestamp(oneBasedIndex, (Timestamp) obj);
					break;
/*				case VerticaDayTimeInterval.INTERVAL_DAY:
				case VerticaDayTimeInterval.INTERVAL_DAY_TO_HOUR:
				case VerticaDayTimeInterval.INTERVAL_DAY_TO_MINUTE:
				case VerticaDayTimeInterval.INTERVAL_DAY_TO_SECOND:
				case VerticaDayTimeInterval.INTERVAL_HOUR:
				case VerticaDayTimeInterval.INTERVAL_HOUR_TO_MINUTE:
				case VerticaDayTimeInterval.INTERVAL_HOUR_TO_SECOND:
				case VerticaDayTimeInterval.INTERVAL_MINUTE:
				case VerticaDayTimeInterval.INTERVAL_MINUTE_TO_SECOND:
				case VerticaDayTimeInterval.INTERVAL_SECOND:
					stmt.setObject(i, obj);
					break;
*/
				default:
					throw new SQLException("Vertica Connector does not know how to process " 
							+ types.get(i)
							+ " for column "
							+ names.get(i)
						);
			}
		}
		stmt.addBatch();
	}
}

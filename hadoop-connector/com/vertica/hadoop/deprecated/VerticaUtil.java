/* Copyright (c) 2005 - 2012 Vertica, an HP company -*- Java -*- */

package com.vertica.hadoop.deprecated;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import com.vertica.hadoop.VerticaConfiguration;
import com.vertica.hadoop.Relation;

public class VerticaUtil {
	private static final Log LOG = LogFactory.getLog("com.vertica.hadoop");

  // TODO: catch when params required but missing
  // TODO: better error message when count query is bad
  public static InputSplit[] getSplits(JobConf job, int num_splits)
      throws IOException {
    LOG.info("creating splits up to " + num_splits);
    HashSet<InputSplit> splits = new HashSet<InputSplit>();
    int i = 0;
    long start = 0;
    long end = 0;
    boolean limit_offset = true;

    // This is the fancy part of mapping inputs...here's how we figure out
    // splits
    // get the params query or the params
    VerticaConfiguration config = new VerticaConfiguration(job);
    String input_query = config.getInputQuery();

    if (input_query == null)
      throw new IOException("Vertica input requires query defined by "
          + VerticaConfiguration.QUERY_PROP);

    String params_query = config.getParamsQuery();
    Collection<List<Object>> params = config.getInputParameters();

    // TODO: limit needs order by unique key
    // TODO: what if there are more parameters than numsplits?
    // prep a count(*) wrapper query and then populate the bind params for each
    String count_query = "SELECT COUNT(*) FROM (\n" + input_query + "\n) count";

    if (params_query != null) {
      LOG.info("creating splits using params_query :" + params_query);
      Connection conn = null;
      Statement stmt = null;
      try {
        conn = config.getConnection(false);
        stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(params_query);
        ResultSetMetaData rsmd = rs.getMetaData();
        while (rs.next()) {
          limit_offset = false;
          List<Object> segment_params = new ArrayList<Object>();
          for (int j = 1; j <= rsmd.getColumnCount(); j++) {
            segment_params.add(rs.getObject(j));
          }
          splits.add(new VerticaInputSplit(input_query, segment_params, start,
              end));
        }
      } catch (Exception e) {
        throw new IOException(e);
      } finally {
        try {
          if (stmt != null)
            stmt.close();
        } catch (SQLException e) {
          throw new IOException(e);
        }
      }
    } else if (params != null && params.size() > 0) {
      LOG.info("creating splits using " + params.size() + " params");
      limit_offset = false;
      for (List<Object> segment_params : params) {
        // if there are more numSplits than params we're going to introduce some
        // limit and offsets
        // TODO: write code to generate the start/end pairs for each group
        splits.add(new VerticaInputSplit(input_query, segment_params, start,
            end));
      }
    }

    if (limit_offset) {
      LOG.info("creating splits using limit and offset");
      Connection conn = null;
      Statement stmt = null;
      long count = 0;

      try {
        conn = config.getConnection(false);
        stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(count_query);
        rs.next();
        count = rs.getLong(1);
      } catch (Exception e) {
        throw new IOException(e);
      } finally {
        try {
          if (stmt != null)
            stmt.close();
        } catch (SQLException e) {
          throw new IOException(e);
        }
      }

      long split_size = count / num_splits;
      end = split_size;

      LOG.info("creating " + num_splits + " splits for " + count + " records");

      for (i = 0; i < num_splits; i++) {
        splits.add(new VerticaInputSplit(input_query, null, start, end));
        start += split_size;
        end += split_size;
      }
    }

    LOG.info("returning " + splits.size() + " final splits");
    return splits.toArray(new InputSplit[0]);
  }
}

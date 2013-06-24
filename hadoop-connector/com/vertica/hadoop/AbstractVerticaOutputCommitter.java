/*
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Abstract OutputCommitter to extend when implementing a custom OutputCommitter. Contains empty
 * implementations of methods to allow easy overriding of specific behavior, as well as some common
 * SQL lifecycle methods.
 *
 * Implementers should consider subclassing @{link VerticaTaskOutputCommitter} instead of this class.
 */
public abstract class AbstractVerticaOutputCommitter extends OutputCommitter {
  protected final Log log = LogFactory.getLog(getClass());

  private final VerticaOutputFormat verticaOutputFormat;

  /**
   * Initializes the OutputCommitter with a handle to the VerticaOutputFormat so the connection can
   * be lazy-initialized on demand. Custom subclasses configured for a job will be initialized via
   * reflection using this constructor.
   *
   * @param verticaOutputFormat the VerticaOutputFormat object
   */
  public AbstractVerticaOutputCommitter(VerticaOutputFormat verticaOutputFormat) {
    if (verticaOutputFormat == null) {
      throw new IllegalArgumentException("VerticaOutputFormat must not be null");
    }
    this.verticaOutputFormat = verticaOutputFormat;
  }

  /**
   * Fetches and returns the connection from the VerticaOutputFormat. The connection will be lazily
   * opened and cached.
   * @param configuration the configuraton to use for the connection
   * @return connection object
   * @throws IOException if the connection couldn't be fetched
   */
  protected Connection getConnection(Configuration configuration) throws IOException {
    return verticaOutputFormat.getConnection(configuration);
  }

  @Override
  public void setupJob(JobContext jobContext) throws IOException { }

  /**
   * This method is will only be called once per job upon a final runstate of
   * {@link org.apache.hadoop.mapreduce.JobStatus.State#SUCCEEDED}. If this method throws an
   * exception the entire job will fail.
   */
  @Override
  public void commitJob(JobContext jobContext) throws IOException { }

  /**
   * This method will be called for a final runstate of
   * {@link org.apache.hadoop.mapreduce.JobStatus.State#FAILED} or
   * {@link org.apache.hadoop.mapreduce.JobStatus.State#KILLED} and might be called more than once.
   */
  @Override
  public void abortJob(JobContext jobContext, JobStatus.State state) throws IOException { }

  @Override
  public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException { }

  /**
   * This method is called upon successful task execution. From the {@link OutputCommitter} javadocs,
   * under very rare circumstances this may be called multiple times for the same task, but for
   * different task attempts.
   */
  @Override
  public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException { }

  /**
   * This method is called upon aborted task execution. This may be called multiple times for the
   * same task, but for different task attempts.
   */
  @Override
  public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException { }


  // Final helper methods below.

  /**
   * Commits and closes the connection if the connection is non-null and open.
   * @throws IOException if either isClosed or commit fails.
   */
  protected final void sqlCommit(Connection connection) throws IOException {
    try {
      if (connection == null || connection.isClosed()) {
        throw new IOException("Trying to commit a connection that is null or closed: " + connection);
      }
    } catch (SQLException e) {
      throw new IOException("Exception calling isClosed on connection", e);
    }

    try {
      connection.commit();
    } catch (SQLException e) {
      throw new IOException("Exception committing connection", e);
    } finally {
      sqlClose(connection);
    }
  }

  /**
   * Rolls back and closes the connection if the connection is non-null and open.
   * @throws IOException if either isClosed or rollback fails.
   */
  protected final void sqlRollback(Connection connection) throws IOException {

    try {
      if (connection == null || connection.isClosed()) {
        throw new IOException("Trying to rollback a connection that is null or closed: " + connection);
      }
    } catch (SQLException e) {
      throw new IOException("Exception calling isClosed on connection", e);
    }

    try {
      connection.rollback();
    } catch (SQLException e) {
      throw new IOException("Exception rolling back connection on table", e);
    } finally {
      sqlClose(connection);
    }
  }

  /**
   * Closes the connection
   */
  protected final void sqlClose(Connection connection) {
    try {
      connection.close();
    } catch (SQLException e) {
      log.warn("Exception closing database connection", e);
    }
  }
}

package com.vertica.hadoop;

import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Class that knows how to commit or rollback SQL connections at the task level. This class can be
 * subclassed to handle specific commit/rollback actions upon job completion.
 * @see VerticaConfiguration.
 */
public class VerticaTaskOutputCommitter extends AbstractVerticaOutputCommitter {

  public VerticaTaskOutputCommitter(VerticaOutputFormat verticaOutputFormat) {
    super(verticaOutputFormat);
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
    return true;
  }

  /**
   * This method is called upon successful task execution. From the {@link OutputCommitter} javadocs,
   * under very rare circumstances this may be called multiple times for the same task, but for
   * different task attempts.
   */
  @Override
  public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {
    log.info("Task complete - committing database connection");
    sqlCommit(getConnection(taskAttemptContext.getConfiguration()));
  }

  /**
   * This method is called upon aborted task execution. This may be called multiple times for the
   * same task, but for different task attempts.
   */
  @Override
  public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {
    log.warn("Task aborted - rolling back database connection");
    sqlRollback(getConnection(taskAttemptContext.getConfiguration()));
  }
}

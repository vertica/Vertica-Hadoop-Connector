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

import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Class that knows how to commit or rollback SQL connections at the task level. This class can be
 * subclassed to handle specific commit/rollback actions upon job completion.
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

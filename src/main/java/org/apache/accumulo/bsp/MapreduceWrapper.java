/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.bsp;

import java.io.IOException;

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hama.bsp.BSPJob;

/**
 * <p>
 * MapreduceWrapper class. Provides a wrapper to wrap {@link BSPJob} into the appropriate Hadoop type required by {@link AccumuloInputFormat} and
 * {@link AccumuloOutputFormat} static configurator methods. Useful for reusing code to set the job's configuration and not using the expected Hadoop API.
 * </p>
 */
public class MapreduceWrapper {
  
  /**
   * Wraps a {@link BSPJob} for reading its {@link Configuration} within Accumulo MapReduce classes' protected static configuration getters.
   * 
   * @param job
   *          the {@link BSPJob} instance to be wrapped
   * @return an instance of {@link TaskAttemptContext} whose {@link Configuration} is the same as the job
   */
  public static TaskAttemptContext wrappedTaskAttemptContext(final BSPJob job) {
    return new TaskAttemptContext(job.getConfiguration(), new TaskAttemptID());
  }
  
  /**
   * Wraps a {@link BSPJob} for writing its {@link Configuration} within Accumulo MapReduce classes' public static configuration setters.
   * 
   * @param job
   *          the {@link BSPJob} instance to be wrapped
   * @return an instance of {@link Job} that exposes {@link BSPJob#getConfiguration()} via {@link Job#getConfiguration()}; no other methods of {@link Job} are
   *         implemented, so this object cannot be used for anything other than editing the {@link BSPJob}'s {@link Configuration}
   */
  public static Job wrappedJob(BSPJob job) {
    final BSPJob bspJob = job;
    try {
      return new Job() {
        @Override
        public Configuration getConfiguration() {
          return bspJob.getConfiguration();
        }
      };
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

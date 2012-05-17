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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.OutputFormat;
import org.apache.hama.bsp.RecordWriter;

public class AccumuloOutputFormat extends org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat implements OutputFormat<Text,Mutation> {
  
  protected static class BSPRecordWriter extends AccumuloRecordWriter implements RecordWriter<Text,Mutation> {
    BSPRecordWriter(Configuration conf) throws AccumuloException, AccumuloSecurityException, IOException {
      super(conf);
    }
    
    /*
     * @see org.apache.hama.bsp.RecordWriter#close()
     */
    @Override
    public void close() throws IOException {
      try {
        close(null);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
    
  }
  
  /*
   * @see org.apache.hama.bsp.OutputFormat#checkOutputSpecs(org.apache.hadoop.fs.FileSystem, org.apache.hama.bsp.BSPJob)
   */
  @Override
  public void checkOutputSpecs(FileSystem fs, BSPJob job) throws IOException {
    checkOutputSpecs(job.getConf());
  }
  
  /*
   * @see org.apache.hama.bsp.OutputFormat#getRecordWriter(org.apache.hadoop.fs.FileSystem, org.apache.hama.bsp.BSPJob, java.lang.String)
   */
  @Override
  public RecordWriter<Text,Mutation> getRecordWriter(FileSystem fs, BSPJob job, String arg2) throws IOException {
    try {
      return new BSPRecordWriter(job.getConf());
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}

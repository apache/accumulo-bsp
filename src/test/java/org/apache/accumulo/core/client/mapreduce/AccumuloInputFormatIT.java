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
package org.apache.accumulo.core.client.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.accumulo.bsp.AccumuloInputFormat;
import org.apache.accumulo.bsp.MapreduceWrapper;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.InputSplit;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.util.KeyValuePair;
import org.junit.Test;

/**
 * 
 */
public class AccumuloInputFormatIT {
  
  static class InputFormatTestBSP<M extends Writable> extends BSP<Key,Value,Key,Value,M> {
    Key key = null;
    int count = 0;
    
    @Override
    public void bsp(BSPPeer<Key,Value,Key,Value,M> peer) throws IOException, SyncException, InterruptedException {
      // this method reads the next key value record from file
      KeyValuePair<Key,Value> pair;
      
      while ((pair = peer.readNext()) != null) {
        if (key != null) {
          assertEquals(key.getRow().toString(), new String(pair.getValue().get()));
        }
        
        assertEquals(pair.getKey().getRow(), new Text(String.format("%09x", count + 1)));
        assertEquals(new String(pair.getValue().get()), String.format("%09x", count));
        count++;
        
        key = new Key(pair.getKey());
      }
      
      peer.sync();
      assertEquals(100, count);
    }
  }
  
  @Test
  public void testBSPInputFormat() throws Exception {
    MockInstance mockInstance = new MockInstance("testmapinstance");
    Connector c = mockInstance.getConnector("root", new byte[] {});
    if (c.tableOperations().exists("testtable"))
      c.tableOperations().delete("testtable");
    c.tableOperations().create("testtable");
    
    BatchWriter bw = c.createBatchWriter("testtable", new BatchWriterConfig());
    for (int i = 0; i < 100; i++) {
      Mutation m = new Mutation(new Text(String.format("%09x", i + 1)));
      m.put(new Text(), new Text(), new Value(String.format("%09x", i).getBytes()));
      bw.addMutation(m);
    }
    bw.close();
    
    BSPJob bspJob = new BSPJob();
    Job job = MapreduceWrapper.wrappedJob(bspJob);
    
    bspJob.setInputFormat(AccumuloInputFormat.class);
    bspJob.setBspClass(InputFormatTestBSP.class);
    bspJob.setInputPath(new Path("test"));
    
    AccumuloInputFormat.setConnectorInfo(job, "root", "".getBytes(Charset.forName("UTF-8")));
    AccumuloInputFormat.setInputTableName(job, "testtable");
    AccumuloInputFormat.setMockInstance(job, "testmapinstance");
    
    AccumuloInputFormat input = new AccumuloInputFormat();
    InputSplit[] splits = input.getSplits(bspJob, 0);
    assertEquals(splits.length, 1);
    
    bspJob.setJar("target/integration-tests.jar");
    bspJob.setOutputPath(new Path("target/bsp-inputformat-test"));
    if (!bspJob.waitForCompletion(false))
      fail("Job not finished successfully");
  }
  
}

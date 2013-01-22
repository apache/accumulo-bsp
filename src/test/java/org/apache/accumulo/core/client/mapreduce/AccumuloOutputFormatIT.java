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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.bsp.AccumuloInputFormat;
import org.apache.accumulo.bsp.AccumuloOutputFormat;
import org.apache.accumulo.bsp.MapreduceWrapper;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hama.HamaConfiguration;
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
public class AccumuloOutputFormatIT {
  
  static class OutputFormatTestBSP<M extends Writable> extends BSP<Key,Value,Text,Mutation,M> {
    Key key = null;
    int count = 0;
    
    @Override
    public void bsp(BSPPeer<Key,Value,Text,Mutation,M> peer) throws IOException, SyncException, InterruptedException {
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
    }
    
    @Override
    public void cleanup(BSPPeer<Key,Value,Text,Mutation,M> peer) throws IOException {
      Mutation m = new Mutation("total");
      m.put("", "", Integer.toString(count));
      peer.write(new Text("testtable2"), m);
    }
  }
  
  @Test
  public void testBSPOutputFormat() throws Exception {
    MockInstance mockInstance = new MockInstance("testmrinstance");
    Connector c = mockInstance.getConnector("root", new byte[] {});
    if (c.tableOperations().exists("testtable1"))
      c.tableOperations().delete("testtable1");
    if (c.tableOperations().exists("testtable2"))
      c.tableOperations().delete("testtable2");
    
    c.tableOperations().create("testtable1");
    c.tableOperations().create("testtable2");
    BatchWriter bw = c.createBatchWriter("testtable1", new BatchWriterConfig());
    for (int i = 0; i < 100; i++) {
      Mutation m = new Mutation(new Text(String.format("%09x", i + 1)));
      m.put(new Text(), new Text(), new Value(String.format("%09x", i).getBytes()));
      bw.addMutation(m);
    }
    bw.close();
    
    Configuration conf = new Configuration();
    BSPJob bspJob = new BSPJob(new HamaConfiguration(conf));
    bspJob.setJobName("Test Input Output");
    
    bspJob.setBspClass(OutputFormatTestBSP.class);
    bspJob.setInputFormat(AccumuloInputFormat.class);
    bspJob.setInputPath(new Path("test"));
    
    bspJob.setOutputFormat(AccumuloOutputFormat.class);
    bspJob.setJar("target/integration-tests.jar");
    bspJob.setOutputPath(new Path("target/bsp-outputformat-test"));
    
    bspJob.setOutputKeyClass(Text.class);
    bspJob.setOutputValueClass(Mutation.class);
    
    Job job = MapreduceWrapper.wrappedJob(bspJob);
    
    AccumuloInputFormat.setConnectorInfo(job, "root", "".getBytes(Charset.forName("UTF-8")));
    AccumuloInputFormat.setInputTableName(job, "testtable1");
    AccumuloInputFormat.setMockInstance(job, "testmrinstance");
    AccumuloOutputFormat.setConnectorInfo(job, "root", "".getBytes(Charset.forName("UTF-8")));
    AccumuloOutputFormat.setDefaultTableName(job, "testtable2");
    AccumuloOutputFormat.setMockInstance(job, "testmrinstance");
    
    AccumuloInputFormat input = new AccumuloInputFormat();
    InputSplit[] splits = input.getSplits(bspJob, 0);
    assertEquals(splits.length, 1);
    
    if (!bspJob.waitForCompletion(false))
      fail("Job not finished successfully");
    
    Scanner scanner = c.createScanner("testtable2", new Authorizations());
    Iterator<Entry<Key,Value>> iter = scanner.iterator();
    assertTrue(iter.hasNext());
    Entry<Key,Value> entry = iter.next();
    assertEquals("total", entry.getKey().getRow().toString());
    assertEquals(100, Integer.parseInt(new String(entry.getValue().get())));
    assertFalse(iter.hasNext());
  }
  
}

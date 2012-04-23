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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.bsp.AccumuloInputFormat;
import org.apache.accumulo.bsp.AccumuloOutputFormat;
import org.apache.accumulo.core.client.BatchWriter;
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
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.InputSplit;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.util.KeyValuePair;
import org.junit.Test;

public class AccumuloOutputFormatTest {
  
  static class TestBSP extends BSP<Key,Value,Text,Mutation> {
    Key key = null;
    int count = 0;
    
    @Override
    public void bsp(BSPPeer<Key,Value,Text,Mutation> peer) throws IOException, SyncException, InterruptedException {
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
    public void cleanup(BSPPeer<Key,Value,Text,Mutation> peer) throws IOException {
      Mutation m = new Mutation("total");
      m.put("", "", Integer.toString(count));
      peer.write(new Text("testtable2"), m);
    }
  }
  
  @Test
  public void testBSP() throws Exception {
    MockInstance mockInstance = new MockInstance("testmrinstance");
    Connector c = mockInstance.getConnector("root", new byte[] {});
    if (c.tableOperations().exists("testtable1"))
      c.tableOperations().delete("testtable1");
    if (c.tableOperations().exists("testtable2"))
      c.tableOperations().delete("testtable2");
    
    c.tableOperations().create("testtable1");
    c.tableOperations().create("testtable2");
    BatchWriter bw = c.createBatchWriter("testtable1", 10000L, 1000L, 4);
    for (int i = 0; i < 100; i++) {
      Mutation m = new Mutation(new Text(String.format("%09x", i + 1)));
      m.put(new Text(), new Text(), new Value(String.format("%09x", i).getBytes()));
      bw.addMutation(m);
    }
    bw.close();
    
    Configuration conf = new Configuration();
    BSPJob bsp = new BSPJob(new HamaConfiguration(conf));
    bsp.setJobName("Test Input Output");
    
    bsp.setBspClass(TestBSP.class);
    bsp.setInputFormat(AccumuloInputFormat.class);
    bsp.setInputPath(new Path("test"));
    
    bsp.setOutputFormat(AccumuloOutputFormat.class);
    bsp.setOutputPath(new Path("test"));
    
    bsp.setOutputKeyClass(Text.class);
    bsp.setOutputValueClass(Mutation.class);
    
    AccumuloInputFormat.setInputInfo(bsp.getConf(), "root", "".getBytes(), "testtable1", new Authorizations());
    AccumuloInputFormat.setMockInstance(bsp.getConf(), "testmrinstance");
    AccumuloOutputFormat.setOutputInfo(bsp.getConf(), "root", "".getBytes(), false, "testtable2");
    AccumuloOutputFormat.setMockInstance(bsp.getConf(), "testmrinstance");
    
    AccumuloInputFormat input = new AccumuloInputFormat();
    InputSplit[] splits = input.getSplits(bsp, 0);
    assertEquals(splits.length, 1);
    
    bsp.waitForCompletion(false);
    
    Scanner scanner = c.createScanner("testtable2", new Authorizations());
    Iterator<Entry<Key,Value>> iter = scanner.iterator();
    assertTrue(iter.hasNext());
    Entry<Key,Value> entry = iter.next();
    assertEquals("total", entry.getKey().getRow().toString());
    assertEquals(100, Integer.parseInt(new String(entry.getValue().get())));
    assertFalse(iter.hasNext());
    
  }
}

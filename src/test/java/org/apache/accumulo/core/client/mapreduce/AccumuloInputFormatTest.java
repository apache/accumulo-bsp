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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.bsp.AccumuloInputFormat;
import org.apache.accumulo.bsp.MapreduceWrapper;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hama.bsp.BSPJob;
import org.junit.Test;

/**
 * 
 */
public class AccumuloInputFormatTest {
  
  @Test
  public void testSetIterator() throws IOException {
    BSPJob bspJob = new BSPJob();
    
    Job job = MapreduceWrapper.wrappedJob(bspJob);
    AccumuloInputFormat.addIterator(job, new IteratorSetting(1, "WholeRow", "org.apache.accumulo.core.iterators.WholeRowIterator"));
    
    TaskAttemptContext context = MapreduceWrapper.wrappedTaskAttemptContext(bspJob);
    List<IteratorSetting> iterators = AccumuloInputFormat.getIterators(context);
    assertEquals(1, iterators.size());
    IteratorSetting iter = iterators.get(0);
    assertEquals(1, iter.getPriority());
    assertEquals("org.apache.accumulo.core.iterators.WholeRowIterator", iter.getIteratorClass());
    assertEquals("WholeRow", iter.getName());
    assertEquals(0, iter.getOptions().size());
  }
  
  @Test
  public void testAddIterator() throws IOException {
    BSPJob bspJob = new BSPJob();
    
    Job job = MapreduceWrapper.wrappedJob(bspJob);
    AccumuloInputFormat.addIterator(job, new IteratorSetting(1, "WholeRow", WholeRowIterator.class));
    AccumuloInputFormat.addIterator(job, new IteratorSetting(2, "Versions", "org.apache.accumulo.core.iterators.VersioningIterator"));
    IteratorSetting iter = new IteratorSetting(3, "Count", "org.apache.accumulo.core.iterators.CountingIterator");
    iter.addOption("v1", "1");
    iter.addOption("junk", "\0omg:!\\xyzzy");
    AccumuloInputFormat.addIterator(job, iter);
    
    TaskAttemptContext context = MapreduceWrapper.wrappedTaskAttemptContext(bspJob);
    List<IteratorSetting> list = AccumuloInputFormat.getIterators(context);
    
    // Check the list size
    assertTrue(list.size() == 3);
    
    // Walk the list and make sure our settings are correct
    IteratorSetting setting = list.get(0);
    assertEquals(1, setting.getPriority());
    assertEquals("org.apache.accumulo.core.iterators.user.WholeRowIterator", setting.getIteratorClass());
    assertEquals("WholeRow", setting.getName());
    
    setting = list.get(1);
    assertEquals(2, setting.getPriority());
    assertEquals("org.apache.accumulo.core.iterators.VersioningIterator", setting.getIteratorClass());
    assertEquals("Versions", setting.getName());
    
    setting = list.get(2);
    assertEquals(3, setting.getPriority());
    assertEquals("org.apache.accumulo.core.iterators.CountingIterator", setting.getIteratorClass());
    assertEquals("Count", setting.getName());
    
    Map<String,String> iteratorOptions = setting.getOptions();
    assertEquals(2, iteratorOptions.size());
    assertTrue(iteratorOptions.containsKey("v1"));
    assertEquals("1", iteratorOptions.get("v1"));
    assertTrue(iteratorOptions.containsKey("junk"));
    assertEquals("\0omg:!\\xyzzy", iteratorOptions.get("junk"));
  }
  
  @Test
  public void testIteratorOptionEncoding() throws IOException {
    BSPJob bspJob = new BSPJob();
    String key = "colon:delimited:key";
    String value = "comma,delimited,value";
    
    Job job = MapreduceWrapper.wrappedJob(bspJob);
    IteratorSetting someSetting = new IteratorSetting(1, "iterator", "Iterator.class");
    someSetting.addOption(key, value);
    AccumuloInputFormat.addIterator(job, someSetting);
    
    TaskAttemptContext context = MapreduceWrapper.wrappedTaskAttemptContext(bspJob);
    List<IteratorSetting> iters = AccumuloInputFormat.getIterators(context);
    assertEquals(1, iters.size());
    assertEquals("iterator", iters.get(0).getName());
    assertEquals("Iterator.class", iters.get(0).getIteratorClass());
    assertEquals(1, iters.get(0).getPriority());
    Map<String,String> opts = iters.get(0).getOptions();
    assertEquals(1, opts.size());
    assertTrue(opts.containsKey(key));
    assertEquals(value, opts.get(key));
    
    someSetting.addOption(key + "2", value);
    someSetting.setPriority(2);
    someSetting.setName("it2");
    AccumuloInputFormat.addIterator(job, someSetting);
    
    context = MapreduceWrapper.wrappedTaskAttemptContext(bspJob);
    iters = AccumuloInputFormat.getIterators(context);
    assertEquals(2, iters.size());
    assertEquals("iterator", iters.get(0).getName());
    assertEquals("Iterator.class", iters.get(0).getIteratorClass());
    assertEquals(1, iters.get(0).getPriority());
    opts = iters.get(0).getOptions();
    assertEquals(1, opts.size());
    assertTrue(opts.containsKey(key));
    assertEquals(value, opts.get(key));
    assertEquals("it2", iters.get(1).getName());
    assertEquals("Iterator.class", iters.get(1).getIteratorClass());
    assertEquals(2, iters.get(1).getPriority());
    opts = iters.get(1).getOptions();
    assertEquals(2, opts.size());
    assertTrue(opts.containsKey(key));
    assertEquals(value, opts.get(key));
    assertTrue(opts.containsKey(key + "2"));
    assertEquals(value, opts.get(key + "2"));
  }
  
  @Test
  public void testGetIteratorSettings() throws IOException {
    BSPJob bspJob = new BSPJob();
    Job job = MapreduceWrapper.wrappedJob(bspJob);
    
    AccumuloInputFormat.addIterator(job, new IteratorSetting(1, "WholeRow", "org.apache.accumulo.core.iterators.WholeRowIterator"));
    AccumuloInputFormat.addIterator(job, new IteratorSetting(2, "Versions", "org.apache.accumulo.core.iterators.VersioningIterator"));
    AccumuloInputFormat.addIterator(job, new IteratorSetting(3, "Count", "org.apache.accumulo.core.iterators.CountingIterator"));
    
    TaskAttemptContext context = MapreduceWrapper.wrappedTaskAttemptContext(bspJob);
    List<IteratorSetting> list = AccumuloInputFormat.getIterators(context);
    
    // Check the list size
    assertEquals(3, list.size());
    
    // Walk the list and make sure our settings are correct
    IteratorSetting setting = list.get(0);
    assertEquals(1, setting.getPriority());
    assertEquals("org.apache.accumulo.core.iterators.WholeRowIterator", setting.getIteratorClass());
    assertEquals("WholeRow", setting.getName());
    
    setting = list.get(1);
    assertEquals(2, setting.getPriority());
    assertEquals("org.apache.accumulo.core.iterators.VersioningIterator", setting.getIteratorClass());
    assertEquals("Versions", setting.getName());
    
    setting = list.get(2);
    assertEquals(3, setting.getPriority());
    assertEquals("org.apache.accumulo.core.iterators.CountingIterator", setting.getIteratorClass());
    assertEquals("Count", setting.getName());
  }
  
  @Test
  public void testSetRegex() throws IOException {
    BSPJob bspJob = new BSPJob();
    Job job = MapreduceWrapper.wrappedJob(bspJob);
    
    String regex = ">\"*%<>\'\\";
    
    IteratorSetting is = new IteratorSetting(50, regex, RegExFilter.class);
    RegExFilter.setRegexs(is, regex, null, null, null, false);
    AccumuloInputFormat.addIterator(job, is);
    
    TaskAttemptContext context = MapreduceWrapper.wrappedTaskAttemptContext(bspJob);
    assertEquals(regex, AccumuloInputFormat.getIterators(context).get(0).getName());
  }
  
}

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
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.InputFormat;
import org.apache.hama.bsp.InputSplit;
import org.apache.hama.bsp.RecordReader;

/**
 * <p>
 * AccumuloInputFormat class. To be used with Hama BSP.
 * </p>
 * 
 * @see BSPJob#setInputFormat(Class)
 */
public class AccumuloInputFormat extends org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat implements InputFormat<Key,Value> {
  
  public class BSPRecordReaderBase extends RecordReaderBase<Key,Value> implements RecordReader<Key,Value> {
    public BSPRecordReaderBase(InputSplit split, BSPJob job) throws IOException {
      this.initialize((BSPRangeInputSplit) split, MapreduceWrapper.wrappedTaskAttemptContext(job));
    }
    
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return next(currentKey, currentValue);
    }
    
    @Override
    public Key createKey() {
      if (currentKey == null) {
        return new Key();
      } else {
        return currentKey;
      }
    }
    
    @Override
    public Value createValue() {
      if (currentValue == null) {
        return new Value(new byte[0]);
      } else {
        return currentValue;
      }
    }
    
    @Override
    public long getPos() throws IOException {
      return 0;
    }
    
    @Override
    public boolean next(Key k, Value v) throws IOException {
      if (scannerIterator.hasNext()) {
        ++numKeysRead;
        Entry<Key,Value> entry = scannerIterator.next();
        currentKey = entry.getKey();
        currentValue = entry.getValue();
        k.set(currentKey);
        v.set(currentValue.get());
        return true;
      }
      return false;
    }
  }
  
  public static class BSPRangeInputSplit extends RangeInputSplit implements InputSplit {
    public BSPRangeInputSplit() {
      super();
    }
    
    public BSPRangeInputSplit(RangeInputSplit split) throws IOException {
      super(split);
    }
  }
  
  @Override
  public RecordReader<Key,Value> getRecordReader(InputSplit split, BSPJob job) throws IOException {
    return new BSPRecordReaderBase(split, job);
  }
  
  @Override
  public InputSplit[] getSplits(BSPJob job, int arg1) throws IOException {
    List<org.apache.hadoop.mapreduce.InputSplit> splits = getSplits(MapreduceWrapper.wrappedTaskAttemptContext(job));
    InputSplit[] bspSplits = new BSPRangeInputSplit[splits.size()];
    for (int i = 0; i < splits.size(); i++) {
      bspSplits[i] = new BSPRangeInputSplit((RangeInputSplit) splits.get(i));
    }
    return bspSplits;
  }
}

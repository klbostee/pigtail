/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fm.last.pigtail.test;

import static java.util.regex.Matcher.quoteReplacement;
import static org.apache.pig.ExecType.LOCAL;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

import junit.framework.TestCase;
import org.junit.Test;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.typedbytes.TypedBytesWritable;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
//import org.apache.pig.test.PigExecTestCase;
import org.apache.pig.data.TupleFactory;


//public class TestSequenceFileLoader extends PigExecTestCase  {
public class TestTypedBytesSequenceFileLoader extends TestCase {
  
  private PigServer pigServer;
	private TupleFactory tupleFactory;
  
  @Override
  public void setUp() throws Exception {
  	pigServer = new PigServer(LOCAL);
  	tupleFactory = TupleFactory.getInstance();
  }
  
  private String createSequenceFile(Object[] data) throws IOException {
    File tmpFile = File.createTempFile("test", ".tbseq");
    String tmpFileName = tmpFile.getAbsolutePath();
    
    System.err.println("fileName: "+tmpFileName);
    Path path = new Path("file:///"+tmpFileName);
    JobConf conf = new JobConf();
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    
    TypedBytesWritable key = new TypedBytesWritable();
    TypedBytesWritable value = new TypedBytesWritable();
    SequenceFile.Writer writer = null;
    try {
      writer = SequenceFile.createWriter(fs, conf, path, 
                                         key.getClass(), value.getClass());
      for (int i = 0; i < data.length; i += 2) {
        key.setValue(data[i]);
        value.setValue(data[i+1]);
        writer.append(key, value);
      }
    } finally {
      IOUtils.closeStream(writer);
    }
    
    // fix the file path string on Windows
    String regex = "\\\\";
    String replacement = quoteReplacement("\\\\");
    return tmpFileName.replaceAll(regex, replacement);
  }
  
  private void conductTest(Object[] data, String asClause) throws IOException {
  	pigServer.registerQuery("A = LOAD 'file:" + createSequenceFile(data) + 
  			"' USING fm.last.pigtail.storage.TypedBytesSequenceFileLoader() AS " +
  			asClause + ";");
    Iterator<?> it = pigServer.openIterator("A");
    int tupleCount = 0;
    while (it.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      if (tuple == null)
        break;
      else {
        System.err.println("checking "+data[tupleCount]+" "+data[tupleCount+1]);
        assertEquals(convertDataType(data[tupleCount]), tuple.get(0));
        assertEquals(convertDataType(data[tupleCount+1]), tuple.get(1));
        tupleCount += 2;
      }
    }
    assertEquals(data.length, tupleCount);
  }
  
  @SuppressWarnings("unchecked")
  private Object convertDataType(Object obj) {
  	if (obj instanceof List) {
  		List vector = (List) obj;
  		return tupleFactory.newTuple(vector);
  	}
  	return obj;
  }
  
  @Test
  public void testStringInt() throws IOException {
    Object[] data = {
    	"one", 1,
    	"two", 2,
    	"three", 3
    };
    conductTest(data, "(key:chararray, val:int)");
  }
  
  @Test
  public void testStringVector() throws IOException {
    Object[] data = {
    	"one", new ArrayList<Object>(Arrays.asList(new Object[] {1, "uno"})),
    	"two", new ArrayList<Object>(Arrays.asList(new Object[] {2, "dos"})),
    	"three", new ArrayList<Object>(Arrays.asList(new Object[] {3, "tres"}))
    };
    conductTest(data, "(key:chararray, val:tuple(a:int, b:chararray))");
  }
  
}

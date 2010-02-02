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

package fm.last.pigtail.storage;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.typedbytes.TypedBytesInput;
import org.apache.hadoop.typedbytes.TypedBytesWritable;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.pig.ExecType;
import org.apache.pig.LoadFunc;
import org.apache.pig.SamplableLoader;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class TypedBytesSequenceFileLoader implements LoadFunc, SamplableLoader {

  private SequenceFile.Reader reader = null;
  private long end;

  private TypedBytesWritable key, value;

  private TupleFactory tupleFactory = TupleFactory.getInstance();

  public TypedBytesSequenceFileLoader() {
    // nothing to do here
  }

  public void bindTo(String fileName, BufferedPositionedInputStream is,
      long offset, long end) throws IOException {

    if (reader == null) {
      Configuration conf = new Configuration();
      Path path = new Path(fileName);
      FileSystem fs = FileSystem.get(path.toUri(), conf);
      reader = new SequenceFile.Reader(fs, path, conf);
    }

    if (offset != 0)
      reader.sync(offset);

    this.end = end;

    try {
      this.key = (TypedBytesWritable) ReflectionUtils.newInstance(reader
          .getKeyClass(), PigMapReduce.sJobConf);
      this.value = (TypedBytesWritable) ReflectionUtils.newInstance(reader
          .getValueClass(), PigMapReduce.sJobConf);
    } catch (ClassCastException e) {
      throw new RuntimeException(
          "SequenceFile contains non-TypedBytesWritable objects", e);
    }
  }

  @Override
  public Schema determineSchema(String fileName, ExecType execType,
      DataStorage storage) throws IOException {
    // we cannot determine the schema in general
    return null;
  }

  @Override
  public void fieldsToRead(Schema schema) {
    // do nothing
  }

  @Override
  public Tuple getNext() throws IOException {
    if (reader != null && (reader.getPosition() < end || !reader.syncSeen())
        && reader.next(key, value)) {
      Tuple tuple = tupleFactory.newTuple(2);
      tuple.set(0, new DataByteArray(key.getBytes(), 0, key.getLength()));
      tuple.set(1, new DataByteArray(value.getBytes(), 0, value.getLength()));
      return tuple;
    }
    return null;
  }

  @Override
  public long getPosition() throws IOException {
    return reader.getPosition();
  }

  @Override
  public Tuple getSampledTuple() throws IOException {
    return this.getNext();
  }

  @Override
  public long skip(long n) throws IOException {
    long startPos = reader.getPosition();
    reader.sync(startPos + n);
    return reader.getPosition() - startPos;
  }

  @Override
  public DataBag bytesToBag(byte[] b) throws IOException {
    throw new FrontendException("Casting to bag is not supported.");
  }

  @Override
  public String bytesToCharArray(byte[] b) throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(b);
    DataInputStream dis = new DataInputStream(bais);
    TypedBytesInput tbin = TypedBytesInput.get(dis);
    if (tbin.readType() != org.apache.hadoop.typedbytes.Type.STRING) {
      throw new FrontendException("Type code does not correspond to string.");
    }
    return tbin.readString();
  }

  @Override
  public Double bytesToDouble(byte[] b) throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(b);
    DataInputStream dis = new DataInputStream(bais);
    TypedBytesInput tbin = TypedBytesInput.get(dis);
    if (tbin.readType() != org.apache.hadoop.typedbytes.Type.DOUBLE) {
      throw new FrontendException("Type code does not correspond to double.");
    }
    return tbin.readDouble();
  }

  @Override
  public Float bytesToFloat(byte[] b) throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(b);
    DataInputStream dis = new DataInputStream(bais);
    TypedBytesInput tbin = TypedBytesInput.get(dis);
    if (tbin.readType() != org.apache.hadoop.typedbytes.Type.FLOAT) {
      throw new FrontendException("Type code does not correspond to float.");
    }
    return tbin.readFloat();
  }

  @Override
  public Integer bytesToInteger(byte[] b) throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(b);
    DataInputStream dis = new DataInputStream(bais);
    TypedBytesInput tbin = TypedBytesInput.get(dis);
    if (tbin.readType() != org.apache.hadoop.typedbytes.Type.INT) {
      throw new FrontendException("Type code does not correspond to int.");
    }
    return tbin.readInt();
  }

  @Override
  public Long bytesToLong(byte[] b) throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(b);
    DataInputStream dis = new DataInputStream(bais);
    TypedBytesInput tbin = TypedBytesInput.get(dis);
    if (tbin.readType() != org.apache.hadoop.typedbytes.Type.LONG) {
      throw new FrontendException("Type code does not correspond to long.");
    }
    return tbin.readLong();
  }

  @Override
  public Map<String, Object> bytesToMap(byte[] b) throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(b);
    DataInputStream dis = new DataInputStream(bais);
    TypedBytesInput tbin = TypedBytesInput.get(dis);
    if (tbin.readType() != org.apache.hadoop.typedbytes.Type.MAP) {
      throw new FrontendException("Type code does not correspond to map.");
    }
    Map<String, Object> result = new HashMap<String, Object>();
    for (Object item : tbin.readMap().entrySet()) {
      Map.Entry<Object, Object> entry = (Map.Entry<Object, Object>) item;
      result.put(entry.getKey().toString(), entry.getValue());
    }
    return result;
  }

  @Override
  public Tuple bytesToTuple(byte[] b) throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(b);
    DataInputStream dis = new DataInputStream(bais);
    TypedBytesInput tbin = TypedBytesInput.get(dis);
    if (tbin.readType() != org.apache.hadoop.typedbytes.Type.VECTOR) {
      throw new FrontendException("Type code does not correspond to vector.");
    }
    Tuple result = tupleFactory.newTuple();
    for (Object item : tbin.readVector()) {
      result.append(item);
    }
    return result;
  }

}

package de.hpi.isg.mdms.hadoop.cassandra;

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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.UUID;

import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


final class CqlFlinkRecordWriter extends RecordWriter<String, ArrayList<Object>>
implements org.apache.hadoop.mapred.RecordWriter<String,ArrayList<Object>>
{

    private CqlBulkRecordWriter bulkRecordWriter;
    
    
    public CqlFlinkRecordWriter(org.apache.hadoop.mapred.JobConf job, org.apache.hadoop.util.Progressable progress) throws IOException{
    	bulkRecordWriter = new CqlBulkRecordWriter(job, progress);
    }

    public CqlFlinkRecordWriter(final TaskAttemptContext context) throws IOException{
    	bulkRecordWriter = new CqlBulkRecordWriter(context);
   }

	@Override
	public void write(String key, ArrayList<Object> values) throws IOException{
		ArrayList<ByteBuffer> byteValues = new ArrayList<ByteBuffer>();
		for (Object value : values) {
			//String
			if(value.getClass() == String.class){
				String string = (String) value;
				byteValues.add(ByteBuffer.wrap(string.getBytes()));				
			}
			//int
			if (value.getClass() == Integer.class) {
				Integer integer = (Integer) value;
				ByteBuffer intBuff = ByteBuffer.allocate(4);
				intBuff.putInt(integer);
				byteValues.add(intBuff);
			}
			//uuid
			if (value.getClass() == UUID.class){
				UUID uuid = (UUID) value;
				 ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
				 bb.putLong(uuid.getMostSignificantBits());
				 bb.putLong(uuid.getLeastSignificantBits());
				 bb.flip();
				byteValues.add(bb);
			}

		}
		bulkRecordWriter.write(null, byteValues);
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException,
			InterruptedException {
		bulkRecordWriter.close(context);
	}

	@Override
	public void close(Reporter reporter) throws IOException {
		bulkRecordWriter.close(reporter);
		
	}
    
}

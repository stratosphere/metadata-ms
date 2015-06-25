package de.hpi.isg.mdms.hadoop.cassandra;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.cassandra.hadoop.cql3.CqlBulkOutputFormat;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.cassandra.thrift.Mutation;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * This is a wrapper class to enable flink jobs to use the CqlBulkOutputFormat 
 * to write results via hadoop to cassandra.
 *
 *CqlBulkOutputFormat cannot be used directy since flink jobs cannot have outputs with the signature <Object, List<ByteBuffer>>,
 *since ByteBuffer and List are abstract types. 
 *
 */


public class CqlFlinkOutputFormat extends OutputFormat<String, ArrayList<Object>>
	implements org.apache.hadoop.mapred.OutputFormat<String,ArrayList<Object>>

	{
	    private CqlBulkOutputFormat bulkOutputFormat;

		public CqlFlinkOutputFormat() {
		super();
		bulkOutputFormat = new CqlBulkOutputFormat();
//		bulkOutputFormat = new BulkOutputFormat();
		}

		@Override
	    public void checkOutputSpecs(JobContext context)
	    {
	        bulkOutputFormat.checkOutputSpecs(context);
	    }

	    @Override
	    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException
	    {
	    	return bulkOutputFormat.getOutputCommitter(context);
	    }

	    /** Fills the deprecated OutputFormat interface for streaming. */
	    @Deprecated
	    public void checkOutputSpecs(org.apache.hadoop.fs.FileSystem filesystem, org.apache.hadoop.mapred.JobConf job) throws IOException
	    {
	        bulkOutputFormat.checkOutputSpecs(filesystem, job);
	    }

	    /** Fills the deprecated OutputFormat interface for streaming. */
	    @Deprecated
	    public CqlFlinkRecordWriter getRecordWriter(org.apache.hadoop.fs.FileSystem filesystem, org.apache.hadoop.mapred.JobConf job, String name, org.apache.hadoop.util.Progressable progress) throws IOException
	    {
	        return new CqlFlinkRecordWriter(job, progress);
	    }

	    @Override
	    public CqlFlinkRecordWriter getRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException
	    {
	        return new CqlFlinkRecordWriter(context);
	    }

	    public static class NullOutputCommitter extends OutputCommitter
	    {
	        public void abortTask(TaskAttemptContext taskContext) { }

	        public void cleanupJob(JobContext jobContext) { }

	        public void commitTask(TaskAttemptContext taskContext) { }

	        public boolean needsTaskCommit(TaskAttemptContext taskContext)
	        {
	            return false;
	        }

	        public void setupJob(JobContext jobContext) { }

	        public void setupTask(TaskAttemptContext taskContext) { }
	    }
	}


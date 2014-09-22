package com.pearson.blur.client.indexer;



import java.io.IOException;

import org.apache.blur.mapreduce.lib.BlurMutate;
import org.apache.blur.mapreduce.lib.BlurOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

public class BlurCustomOutputFormat extends BlurOutputFormat {
	
	
	 public BlurCustomOutputFormat(){
		 
		 super();
	 }
	
	  @Override
	  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {

	  }
	  
	  @Override
	  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
	    return new BlurOutputCommitter();
	  }
	  
	  @Override
	  public RecordWriter<Text, BlurMutate> getRecordWriter(TaskAttemptContext context) throws IOException,
	      InterruptedException {
	    int id = context.getTaskAttemptID().getTaskID().getId();
	    TaskAttemptID taskAttemptID = context.getTaskAttemptID();
	    final GenericBlurRecordWriter writer = new GenericBlurRecordWriter(context.getConfiguration(), id,
	        taskAttemptID.toString() + ".tmp");
	    return new RecordWriter<Text, BlurMutate>() {

	      @Override
	      public void write(Text key, BlurMutate value) throws IOException, InterruptedException {
	        writer.write(key, value);
	      }

	      @Override
	      public void close(TaskAttemptContext context) throws IOException, InterruptedException {
	        writer.close();
	      }
	    };
	  }	  
	  
	  public static void setOutputPath(Configuration configuration, Path path) {
		    configuration.set(BLUR_OUTPUT_PATH, path.toString());
		    configuration.set("mapred.output.committer.class", BlurOutputCommitter.class.getName());
	  }
}

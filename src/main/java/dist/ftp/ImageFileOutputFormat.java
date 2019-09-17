package dist.ftp;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;

public class ImageFileOutputFormat extends FileOutputFormat<Text, BytesWritable> {

	public ImageFileOutputFormat() {
	}

	@Override
	public org.apache.hadoop.mapreduce.RecordWriter<Text, BytesWritable> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		return new ImageFileRecordWriter(job);
	}

	// override the function existing in FileOutputFormat class to allow writing into existing directory 
	public void checkOutputSpecs(JobContext job) throws IOException {

		// Ensure that the output directory is set and not already there
		Path outDir = getOutputPath(job);
		if (outDir == null) {
			throw new InvalidJobConfException("Output directory not set.");
		}

		// get delegation token for outDir's file system
		TokenCache.obtainTokensForNamenodes(job.getCredentials(), new Path[] { outDir }, job.getConfiguration());
	}

}
package dist.ftp;

import java.io.IOException;


import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ImageFileOutputFormat extends FileOutputFormat<Text, BytesWritable> {

	public ImageFileOutputFormat() {}

	@Override
	public org.apache.hadoop.mapreduce.RecordWriter<Text, BytesWritable> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		return new ImageFileRecordWriter(job);
	}

}
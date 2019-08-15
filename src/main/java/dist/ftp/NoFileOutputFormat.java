package dist.ftp;

import java.io.IOException;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class NoFileOutputFormat extends FileOutputFormat<NullWritable, NullWritable> {

    public NoFileOutputFormat(){}

    @Override
    public org.apache.hadoop.mapreduce.RecordWriter<NullWritable, NullWritable> getRecordWriter(TaskAttemptContext job)
            throws IOException, InterruptedException {
        return null;
	}

}
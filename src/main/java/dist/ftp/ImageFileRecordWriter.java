package dist.ftp;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

class ImageFileRecordWriter extends RecordWriter<Text, BytesWritable> {
    private FileSystem fs;
    private String baseFolder;
    private String parentDir;

    public ImageFileRecordWriter(TaskAttemptContext context) throws IOException {
        fs = FileSystem.get(context.getConfiguration());
        baseFolder = context.getConfiguration().get("base-folder");
        parentDir = FileOutputFormat.getOutputPath(context).toString(); 
    }

    @Override
    public void write(Text key, BytesWritable value) throws IOException, InterruptedException {
        String outputFileName = key.toString().split(baseFolder)[1];
        
        StringBuilder sb = new StringBuilder();
        sb.append(parentDir);
        sb.append("/");
        sb.append(outputFileName);

        FSDataOutputStream out = fs.create(new Path(sb.toString()));
        out.write(value.getBytes());
        out.close();

    }

    @Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {}
    
}
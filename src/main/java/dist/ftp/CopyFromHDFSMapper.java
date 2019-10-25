package dist.ftp;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

class CopyFromHDFSMapper extends Mapper<Object,Text,NullWritable,NullWritable>{
    Logger logger = LogManager.getLogger(CopyFromHDFSMapper.class);
    private FtpUtil ftp;
    private FileSystem hdfs;
    private String baseFolder;
    private String inputFolder;
    private FTPClient ftpClient;

    @Override
    protected void setup(Mapper<Object, Text, NullWritable, NullWritable>.Context context)
        throws IOException, InterruptedException {

        ftpClient = new FTPClient();
        Configuration conf = context.getConfiguration();

        inputFolder = conf.get("input-folder");
        baseFolder = conf.get("base-folder");
        ftpClient.connect(conf.get("ftp-host"));
        ftpClient.login(conf.get("ftp-user-name"), conf.get("ftp-password"));
        ftpClient.enterLocalPassiveMode();
        ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
        ftp = new FtpUtil(ftpClient);

        try {
            hdfs = FileSystem.get(new URI(conf.get("file-system")), conf);
        } catch (URISyntaxException e) {
            e.printStackTrace();
		}
}

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, NullWritable, NullWritable>.Context context)
            throws IOException, InterruptedException {
                
        String ftpFileLocation = baseFolder + value.toString().split(inputFolder + "/")[1];
        FSDataInputStream input = hdfs.open(new Path(value.toString()));
        ftp.uploadFile(ftpFileLocation, IOUtils.toByteArray(input));

    }

    @Override
    protected void cleanup(Mapper<Object, Text, NullWritable, NullWritable>.Context context)
            throws IOException, InterruptedException {
        super.cleanup(context);
        ftpClient.disconnect();
    }
}
package dist.ftp;

import java.io.IOException;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CopyToHDFSMapper extends Mapper<Object,Text,Text,BytesWritable>{
    private FtpUtil ftp;

    @Override
    protected void setup(Mapper<Object, Text, Text, BytesWritable>.Context context)
            throws IOException, InterruptedException {
            FTPClient ftpClient = new FTPClient();
            Configuration conf = context.getConfiguration();
            ftpClient.connect(conf.get("ftp-host"));
            ftpClient.login(conf.get("ftp-user-name"), conf.get("ftp-password"));
            ftpClient.enterLocalPassiveMode();
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
            ftp = new FtpUtil(ftpClient);
    }

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, BytesWritable>.Context context)
            throws IOException, InterruptedException {
            BytesWritable file = new BytesWritable(ftp.downloadFile(value.toString()));
            context.write(value, file);
    }
}
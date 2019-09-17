package dist.ftp;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class App extends Configured implements Tool{
    static Properties configFile = new Properties();

    // convert to localFS for debugging on single user mode
    final static String fileSystem = "hdfs:///";
   
    // Values read from configuration file
    static String ftpHost; 
    static String ftpUsername; 
    static String ftpPassword;
    
    // command line args representing available options
    final static String toHdfs = "tohdfs";
    final static String fromHdfs = "fromhdfs";
    final static Path splitFile = new Path("inputSplit.txt");
    final static Path dummyOutput = new Path("dummy");
   
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJarByClass(App.class);

        Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
        jobConf.setInt("mapreduce.input.lineinputformat.linespermap", 300);
        jobConf.set("ftp-user-name",ftpUsername);
        jobConf.set("ftp-password", ftpPassword);
        jobConf.set("ftp-host",ftpHost);

        // jobConf.set("yarn.app.mapreduce.am.resource.mb","512");
        // jobConf.set("mapreduce.map.memory.mb","256");
        // jobConf.set("mapred.child.java.opts","-Xmx150m");
        

        if(args[0].toLowerCase().equals(toHdfs)){
            job.setJobName("dist-ftp-"+toHdfs);

            String [] temp = args[1].split("/");
            String parent = String.join("/",Arrays.copyOf(temp, temp.length - 1));   
            jobConf.set("base-folder",parent);
            job.setMapperClass(CopyToHDFSMapper.class);
            job.setOutputFormatClass(ImageFileOutputFormat.class);
            ImageFileOutputFormat.setOutputPath(job, new Path(args[2]));
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(BytesWritable.class);

        } else if(args[0].toLowerCase().equals(fromHdfs)){
            job.setJobName("dist-ftp-"+fromHdfs);

            jobConf.set("base-folder",args[2]+"/");
            String inputFolder = args[1];
            int lastCharacterIndex = inputFolder.length() - 1;

            if(inputFolder.charAt(lastCharacterIndex) == '/'){
                inputFolder = inputFolder.substring(0,lastCharacterIndex);
            }

            jobConf.set("input-folder",inputFolder);
            jobConf.set("file-system",fileSystem);
            job.setMapperClass(CopyFromHDFSMapper.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(NullWritable.class);
            job.setOutputFormatClass(NoFileOutputFormat.class);
            NoFileOutputFormat.setOutputPath(job, dummyOutput);
        }
        
        job.setInputFormatClass(NLineInputFormat.class);
        job.setNumReduceTasks(0);
        NLineInputFormat.addInputPath(job, splitFile);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static String getHdfsListing(FileSystem fs,String directory) throws IOException {
        RemoteIterator<LocatedFileStatus> statusIterator = fs.listFiles(new Path(directory), true);
        StringBuilder sb = new StringBuilder();
        while(statusIterator.hasNext()){
            sb.append(statusIterator.next().getPath().toString());
            if(statusIterator.hasNext()){
                sb.append("\n");
            }
        }
        return sb.toString();
    }

    public static String getFtpListing(String directory) throws SocketException, IOException{
        FTPClient ftpClient = new FTPClient();
        FtpUtil ftp = new FtpUtil(ftpClient);
        ftpClient.connect(ftpHost);
        ftpClient.login(ftpUsername, ftpPassword);
        ftpClient.enterLocalPassiveMode();
        ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
        
        return String.join("\n", ftp.listAllSubDirectoriesRecursively(directory));
    }

    public static void cleanup(){

        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(new URI(fileSystem), conf);
            fs.delete(splitFile, true);
            fs.delete(dummyOutput,true);    
        } catch (IOException | URISyntaxException e) {}
        
    }

    public static void main(String[] args) {
        
        try {
            //read configuration file
            configFile.load(new FileInputStream("config.properties"));
            ftpHost = configFile.getProperty("ftpHost");
            ftpUsername = configFile.getProperty("ftpUsername");
            ftpPassword = configFile.getProperty("ftpPassword");

            if(ftpHost == null || ftpUsername == null || ftpPassword == null){
                System.out.println("Invalid ftp configuration");
                System.exit(1);
            }


            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(new URI(fileSystem), conf);
            String listing = null;

            if(args[0].toLowerCase().equals(fromHdfs)){
                listing = getHdfsListing(fs,args[1]);
            } else if (args[0].toLowerCase().equals(toHdfs)){
                listing = getFtpListing(args[1]);
            }
             
            FSDataOutputStream fsOut = fs.create(splitFile);
            fsOut.writeBytes(listing);
            fsOut.close();

            ToolRunner.run(new App(), args);

        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();

        } catch(Exception e){
            e.printStackTrace();

        } finally{
           cleanup();
        }
    
    }
}

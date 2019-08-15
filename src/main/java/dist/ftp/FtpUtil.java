package dist.ftp;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

public class FtpUtil { 
    private FTPClient ftpClient;

    public FtpUtil(FTPClient ftpClient){
        this.ftpClient = ftpClient;
    }

    public List<String> listAllSubDirectoriesRecursively(String baseDirectory) throws IOException{

        ArrayList<String> allFiles = new ArrayList<String>();
        Queue<String> directories = new LinkedList<String>();
       
        directories.add(baseDirectory);
        while(!directories.isEmpty()){
            String dir = directories.remove();
            FTPFile [] files =  ftpClient.listFiles(dir);
            for(FTPFile file : files){
                if(file.getType() == FTPFile.DIRECTORY_TYPE){
                    directories.add(dir + "/" + file.getName());
                } else if(file.getType() == FTPFile.FILE_TYPE){
                    allFiles.add(dir + "/" + file.getName());
                }
            }
        }
        return allFiles;
    }

    public byte[] downloadFile(String file) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ftpClient.retrieveFile(file, out);
        byte[] contents = out.toByteArray();
        out.close();
        return contents;
    }

    public void createDirectoryStructure(String filePath) throws IOException {
        String [] temp = filePath.split("/");
        String[] dirs = Arrays.copyOfRange(temp , 0, temp.length-1);

        // if the required Directory already exists return
        if(ftpClient.listFiles(String.join("/", dirs)).length > 0){
            return;
        }

        temp = null;

        String tmp = "/";
        for(String dir : dirs){
            tmp += dir + "/";
            if(ftpClient.listFiles(tmp).length == 0){
                ftpClient.makeDirectory(tmp);
            } 
        }

    }


    public void uploadFile(String filePath, byte[] content) throws IOException {
        createDirectoryStructure(filePath);
        ByteArrayInputStream input = new ByteArrayInputStream(content);
        ftpClient.storeFile(filePath, input);
        input.close();
    }
}

package com.esmeetu.hof.core;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.ftpserver.ftplet.FileSystemView;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.User;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsFtpFileSystemView implements FileSystemView {
    
    public static final Logger LOGGER = LoggerFactory.getLogger(HdfsFtpFileSystemView.class);

    public static String hdfsUri = "hdfs://localhost:9000/";
    public static String hdfsUser = "hadoop";

    public String current = "/";

    public DistributedFileSystem dfs = null;

    public HdfsFtpFileSystemView(User user) {
        DistributedFileSystem temp = new DistributedFileSystem();
        Configuration conf = new Configuration();
        conf.set("HADOOP_USER_NAME", hdfsUser);
        try {
            temp.initialize(new URI(hdfsUri), conf);
            dfs = temp;
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
            LOGGER.error("FtpFileSystemView|initialize");
        }
    }

    @Override
    public void dispose() {
        if (dfs == null)
            return;

        try {
            dfs.close();
        } catch (IOException e) {
            LOGGER.error("FtpFileSystemView|dispose");
        }
    }

    @Override
    public boolean changeWorkingDirectory(String path) throws FtpException {
        if (path.startsWith("/")) {
            current = path;
        } else if (path.equals("src/main")) {
            current = current.substring(0, current.lastIndexOf("/"));
            if (current.equals(""))
                current = "/";
        } else if (current.endsWith("/")) {
            current = current + path;
        } else {
            current = current + "/" + path;
        }
        return true;
    }

    @Override
    public FtpFile getFile(String file) throws FtpException {
        String path = "";

        if (file.startsWith("/")) {
            path = file;
        } else if (file.equals("./")) {
            path = current;
        } else if (current.equals("/")) {
            path = current + file;
        } else {
            path = current + "/" + file;
        }

        return new HdfsFtpFile(path, this);
    }

    @Override
    public FtpFile getHomeDirectory() throws FtpException {
        return new HdfsFtpFile("/", this);
    }

    @Override
    public FtpFile getWorkingDirectory() throws FtpException {
        return new HdfsFtpFile(current, this);
    }

    @Override
    public boolean isRandomAccessible() throws FtpException {
        return true;
    }
}

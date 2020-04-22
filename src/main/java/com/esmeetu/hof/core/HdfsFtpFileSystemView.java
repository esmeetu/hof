package com.esmeetu.hof.core;

import org.apache.ftpserver.ftplet.FileSystemView;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.User;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsFtpFileSystemView implements FileSystemView {
    
    public static final Logger LOGGER = LoggerFactory.getLogger(HdfsFtpFileSystemView.class);

    public static String hdfsUri = "hdfs://localhost:9000/";
    public static String hdfsUser = "hadoop";

    private User user;

    private String homeDir;
    private String current = "/";

    public DistributedFileSystem dfs = null;

    public HdfsFtpFileSystemView(User user) {
        DistributedFileSystem temp = new DistributedFileSystem();
        Configuration conf = new Configuration();
        conf.set("HADOOP_USER_NAME", hdfsUser);
        this.user = user;
        String homeDir = user.getHomeDirectory();
        if (!homeDir.endsWith("/")) {
            homeDir += '/';
        }
        this.homeDir = homeDir;
        try {
            temp.initialize(new URI(hdfsUri), conf);
            dfs = temp;
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
            LOGGER.error("FtpFileSystemView|initialize");
        }
    }

    public User getUser() {
        return user;
    }

    public String getHomeDir() {
        return homeDir;
    }

    @Override
    public void dispose() {
        if (dfs == null) {
            return;
        }

        try {
            dfs.close();
        } catch (IOException e) {
            LOGGER.error("FtpFileSystemView|dispose");
        }
    }

    @Override
    public boolean changeWorkingDirectory(String dir) throws FtpException {
        String path;
        if (dir.startsWith("/")) {
            path = dir;
        } else if (current.length() > 1) {
            path = current + "/" + dir;
        } else {
            path = dir;
        }
        HdfsFtpFile file = new HdfsFtpFile(path, this);
        if (file.isDirectory()) {
            current = path;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public FtpFile getFile(String file) throws FtpException {
        String path;
        if (file.startsWith("/")) {
            path = file;
        } else if (current.length() > 1) {
            path = current + "/" + file;
        } else {
            path = file;
        }

        return new HdfsFtpFile(path, this);
    }

    @Override
    public FtpFile getHomeDirectory() {
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

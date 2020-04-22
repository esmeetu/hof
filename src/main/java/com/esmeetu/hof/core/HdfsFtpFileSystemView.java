package com.esmeetu.hof.core;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

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

    private String homeDir;
    private String current = "/";

    public DistributedFileSystem dfs = null;

    public HdfsFtpFileSystemView(User user) {
        DistributedFileSystem temp = new DistributedFileSystem();
        Configuration conf = new Configuration();
        conf.set("HADOOP_USER_NAME", hdfsUser);
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
        Path path;
        if (dir.startsWith("/")) {
            path = Paths.get(homeDir,  dir);
        } else if (current.length() > 1) {
            path = Paths.get(current,  dir);
        } else {
            path = Paths.get(homeDir,  dir);
        }
        HdfsFtpFile file = new HdfsFtpFile(path.toString(), this);
        if (file.isDirectory()) {
            current = path.toString();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public FtpFile getFile(String file) throws FtpException {
        Path path;
        if (file.startsWith("/")) {
            path = Paths.get(homeDir,  file);
        } else if (current.length() > 1) {
            path = Paths.get(current,  file);
        } else {
            path = Paths.get(homeDir,  file);
        }

        return new HdfsFtpFile(path.toString(), this);
    }

    @Override
    public FtpFile getHomeDirectory() throws FtpException {
        return new HdfsFtpFile(homeDir, this);
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

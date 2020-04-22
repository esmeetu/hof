package com.esmeetu.hof.core;

import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class HdfsFtpFile implements FtpFile {

    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsFtpFile.class);

    public String path;
    public HdfsFtpFileSystemView view;

    public HdfsFtpFile(String path, HdfsFtpFileSystemView view) {
        this.path = path;
        this.view = view;
    }

    @Override
    public boolean doesExist() {
        try {
            return view.dfs.exists(new Path(path));
        } catch (IllegalArgumentException | IOException e) {
            LOGGER.error("HdfsFtpFile doesExist");
        }
        return false;
    }

    @Override
    public String getAbsolutePath() {
        return path;
    }

    @Override
    public String getGroupName() {
        LOGGER.info("PROCESS HdfsFtpFile getGroupName path :" + path);
        return null;
    }

    @Override
    public int getLinkCount() {
        //unknown used
        LOGGER.info("PROCESS HdfsFtpFile getLinkCount path :" + path);
        return 0;
    }

    @Override
    public String getName() {
        try {
            return view.dfs.getFileStatus(new Path(path)).getPath().getName();
        } catch (IllegalArgumentException | IOException e) {
            LOGGER.error("HdfsFtpFile getName");
        }
        return "undefined";
    }

    @Override
    public String getOwnerName() {
        try {
            return view.dfs.getFileStatus(new Path(path)).getOwner();
        } catch (IllegalArgumentException | IOException e) {
            LOGGER.error("HdfsFtpFile getOwerName");
        }
        return "undefined";
    }

    @Override
    public Object getPhysicalFile() {
        LOGGER.info("PROCESS HdfsFtpFile getPhysicalFile path :" + path);
        return null;
    }

    @Override
    public long getSize() {
        try {
            return view.dfs.getFileStatus(new Path(path)).getLen();
        } catch (IllegalArgumentException | IOException e) {
            LOGGER.error("HdfsFtpFile getSize");
        }
        return 0;
    }

    @Override
    public boolean isDirectory() {
        try {
            return view.dfs.isDirectory(new Path(path));
        } catch (IllegalArgumentException | IOException e) {
            LOGGER.error("HdfsFtpFile isDirectory");
        }
        return false;
    }

    @Override
    public boolean isFile() {
        try {
            return view.dfs.isFile(new Path(path));
        } catch (IllegalArgumentException | IOException e) {
            LOGGER.error("HdfsFtpFile isFile");
        }
        return false;
    }

    // return null?
    private FsPermission getPermissions() {
        try {
            return view.dfs.getFileStatus(new Path(path)).getPermission();
        } catch (IllegalArgumentException | IOException e) {
            LOGGER.error("HdfsFtpFile getPermissions");
        }
        return null;
    }

    @Override
    public long getLastModified() {
        try {
            return view.dfs.getFileStatus(new Path(path)).getModificationTime();
        } catch (IllegalArgumentException | IOException e) {
            LOGGER.error("HdfsFtpFile getLastModified");
        }
        return 0;
    }

    @Override
    public boolean setLastModified(long arg0) {
        LOGGER.error("PROCESS HdfsFtpFile setLastModified path :" + path);
        return false;
    }

    @Override
    public boolean isHidden() {
        return false;
    }

    @Override
    public boolean isReadable() {
        return true;
    }

    @Override
    public boolean isRemovable() {
        return isWritable();
    }

    @Override
    public boolean isWritable() {
        return !"anonymous".equals(view.getUser().getName());
    }

    @Override
    public List<FtpFile> listFiles() {
        try {
            return Arrays.stream(view.dfs.listStatus(new Path(path))).map(v -> new HdfsFtpFile(v.getPath().toString(), view)).collect(Collectors.toList());
        } catch (IllegalArgumentException | IOException e) {
            LOGGER.error("HdfsFtpFile listFiles");
        }
        return new ArrayList<>();

    }

    @Override
    public boolean mkdir() {
        try {
            view.dfs.mkdirs(new Path(path));
            return true;
        } catch (IllegalArgumentException | IOException e) {
            LOGGER.error("HdfsFtpFile mkdir");
        }
        return false;
    }

    @Override
    public boolean move(FtpFile file) {
        try {
            return view.dfs.rename(new Path(path), new Path(file.getAbsolutePath()));
        } catch (IllegalArgumentException | IOException e) {
            LOGGER.error("HdfsFtpFile move");
        }
        return false;
    }

    @Override
    public boolean delete() {
        try {
            return view.dfs.delete(new Path(path), true);
        } catch (IllegalArgumentException | IOException e) {
            LOGGER.error("HdfsFtpFile delete");
        }
        return false;
    }


    @Override
    public InputStream createInputStream(long skipLen) throws IOException {
        FSDataInputStream fis = null;
        try {
            fis = view.dfs.open(new Path(path));
            fis.seek(skipLen);
        } catch (IOException | NullPointerException e) {
            LOGGER.error("HdfsFtpFile createInputStream");
        }

        return fis;
    }

    @Override
    public OutputStream createOutputStream(long arg0) throws IOException {
        try {
            view.dfs.createNewFile(new Path(path));
            return view.dfs.create(new Path(path));
        } catch (IOException e) {
            LOGGER.error("HdfsFtpFile createOutputStream");
        }
        return null;
    }


}

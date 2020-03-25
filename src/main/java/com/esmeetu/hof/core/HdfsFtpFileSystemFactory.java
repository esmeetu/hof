package com.esmeetu.hof.core;

import org.apache.ftpserver.ftplet.FileSystemFactory;
import org.apache.ftpserver.ftplet.FileSystemView;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.User;

public class HdfsFtpFileSystemFactory implements FileSystemFactory {
    @Override
    public FileSystemView createFileSystemView(User user) throws FtpException {
        return new HdfsFtpFileSystemView(user);
    }
}

package com.esmeetu.hof;

import com.esmeetu.hof.core.HdfsFtpFileSystemFactory;
import com.esmeetu.hof.core.HdfsFtpFileSystemView;
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class HdfsFtpServer {

    public static void main(String[] args) {

        Path resourcePath = Paths.get(System.getProperty("user.dir"), "config");

        File serverPropertiesFile = new File(resourcePath.toString(), "server.properties");
        if (!serverPropertiesFile.exists()) {
            System.out.println("Error: server properties not found.");
            return;
        }

        File userPropertiesFile = new File(resourcePath.toString(), "users.properties");
        if (!userPropertiesFile.exists()) {
            System.out.println("Error: user properties not found.");
            return;
        }

        try {
            HdfsFtpServer.run(serverPropertiesFile, userPropertiesFile);
        } catch (FtpException | IOException e) {
            System.out.println("Error: server run failed.");
        }
    }

    public static void run(File serverProperties, File userPropertiesFile) throws IOException, FtpException {
        FtpServerFactory serverFactory = new FtpServerFactory();

        //filesystem conf
        serverFactory.setFileSystem(new HdfsFtpFileSystemFactory());


        //server conf
        Properties properties = new Properties();
        properties.load(new FileInputStream(serverProperties));

        int port = Integer.parseInt(properties.getProperty("port"));
        HdfsFtpFileSystemView.hdfsUri = properties.getProperty("hdfsUri");
        HdfsFtpFileSystemView.hdfsUser = properties.getProperty("hdfsUser");

        ListenerFactory listenerFactory = new ListenerFactory();
        listenerFactory.setPort(port);
        serverFactory.addListener("default", listenerFactory.createListener());

        //user conf
        PropertiesUserManagerFactory userManagerFactory = new PropertiesUserManagerFactory();
        userManagerFactory.setFile(userPropertiesFile);

        serverFactory.setUserManager(userManagerFactory.createUserManager());
        FtpServer server = serverFactory.createServer();

        server.start();
    }
}

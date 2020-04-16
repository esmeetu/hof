package com.esmeetu.hof;

import com.esmeetu.hof.core.FtpMode;
import com.esmeetu.hof.core.HdfsFtpFileSystemFactory;
import com.esmeetu.hof.core.HdfsFtpFileSystemView;
import org.apache.ftpserver.DataConnectionConfigurationFactory;
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.ssl.SslConfigurationFactory;
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class HdfsFtpServer {
    
    public static final Logger LOGGER = LoggerFactory.getLogger(HdfsFtpServer.class);

    private static FtpMode mode = FtpMode.FTP;

    public static void main(String[] args) {

        Path resourcePath = Paths.get(System.getProperty("user.dir"), "config");

        File serverPropertiesFile = new File(resourcePath.toString(), "server.properties");
        if (!serverPropertiesFile.exists()) {
            LOGGER.error("server properties not found.");
            return;
        }

        File userPropertiesFile = new File(resourcePath.toString(), "users.properties");
        if (!userPropertiesFile.exists()) {
            LOGGER.error("user properties not found.");
            return;
        }

        try {
            HdfsFtpServer.run(serverPropertiesFile, userPropertiesFile);
        } catch (FtpException | IOException e) {
            LOGGER.error("server run failed.");
        }
    }

    public static void run(File serverProperties, File userPropertiesFile) throws IOException, FtpException {
        FtpServerFactory serverFactory = new FtpServerFactory();
        //filesystem conf
        serverFactory.setFileSystem(new HdfsFtpFileSystemFactory());

        //user conf
        PropertiesUserManagerFactory userManagerFactory = new PropertiesUserManagerFactory();
        userManagerFactory.setFile(userPropertiesFile);

        serverFactory.setUserManager(userManagerFactory.createUserManager());

        //server conf
        Properties properties = new Properties();
        properties.load(new FileInputStream(serverProperties));

        HdfsFtpFileSystemView.hdfsUri = properties.getProperty("hdfsUri");
        HdfsFtpFileSystemView.hdfsUser = properties.getProperty("hdfsUser");
        HdfsFtpFileSystemView.current = properties.getProperty("hdfsDir");

        ListenerFactory listenerFactory = new ListenerFactory();
        DataConnectionConfigurationFactory dccf = new DataConnectionConfigurationFactory();
        if (mode.equals(FtpMode.FTP)) {
            listenerFactory.setPort(Integer.parseInt(properties.getProperty("port")));
            dccf.setPassivePorts(properties.getProperty("data-ports"));
        } else {
            listenerFactory.setPort(Integer.parseInt(properties.getProperty("ssl-port")));
            dccf.setPassivePorts(properties.getProperty("ssl-data-ports"));

            // ssl
            SslConfigurationFactory ssl = new SslConfigurationFactory();
            // set the SSL configuration for the listener
            ssl.setKeystoreFile(new File(serverProperties.getParent(), "ca/server.jks"));
            ssl.setKeystorePassword("111111");
            listenerFactory.setSslConfiguration(ssl.createSslConfiguration());
            listenerFactory.setImplicitSsl(true);
        }
        listenerFactory.setDataConnectionConfiguration(dccf.createDataConnectionConfiguration());
        serverFactory.addListener("default", listenerFactory.createListener());

        FtpServer server = serverFactory.createServer();

        server.start();
    }
}

# hof
a ftp server over hdfs using ftpserver.

# Compatibility
- Hadoop 2.7.x
- JDK 1.8

# Build
`mvn clean package`

# How to use
1. configure resources files: `server.properties` and `users.properties.`

default ftp user: `admin` password: `admin`. 
default server config: hdfs uri: `localhost:9000`, hdfs user: `esmee`

2. run `com.esmeetu.hof.HdfsFtpServer` in IDE or run build jar-with-dependencies:
`java -jar hof-<version>-jar-with-dependencies.jar`



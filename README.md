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

3. client connect:
```sh
curl --user admin:admin ftps://localhost:2222/path/to/hdfsfile -o /path/to/hdfsfile
```

# FTPS Support
1. gen a `.pem` cert
```$xslt
mkdir config/ca && cd config/ca

# gen a self-signed jks keystore
keytool -genkeypair -alias server -keyalg RSA -dname "CN=localhost" -keystore server.jks -keypass 111111 -storepass 111111

# convert to p12 keystore
keytool -importkeystore -srckeystore server.p12 -destkeystore server.jks -deststoretype jks

# get cert from p12 keystore
openssl pkcs12 -in identity.p12 -nokeys -out cert.pem
```

2. connect ftps server
```sh
curl --user admin:admin --cacert cert.pem ftps://localhost:2222/path/to/hdfsfile -o /path/to/hdfsfile
```
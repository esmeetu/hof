package com.esmeetu.hof.core;

public enum FtpMode {

    FTP("ftp"),
    FTPS("ftps");

    private String mode;

    FtpMode(String mode) {
        this.mode = mode;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }
}

# Hadoop Setup for Spark Windows Developers

Apache Spark lacks a built-in distributed file system for organizing files, so it relies on external file systems to store and process large datasets. As a result, Spark is often installed alongside Hadoop, allowing its advanced analytics applications to utilize data stored in the Hadoop Distributed File System (HDFS). This document describe the process to configure Windows to implement hadoop native calls and being able to create data frames using local data sources.

## Common symptom

Usually developers that use Windows to create Data Frames using Spark face the following exception:

    Caused by: java.lang.UnsatisfiedLinkError: org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(Ljava/lang/String;I)Z
    at org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(Native Method)
    at org.apache.hadoop.io.nativeio.NativeIO$Windows.access(NativeIO.java:645)
    at org.apache.hadoop.fs.FileUtil.canRead(FileUtil.java:1230)
    at org.apache.hadoop.fs.FileUtil.list(FileUtil.java:1435)
    at org.apache.hadoop.fs.RawLocalFileSystem.listStatus(RawLocalFileSystem.java:493)
    at org.apache.hadoop.fs.FileSystem.listStatus(FileSystem.java:1868)
    at org.apache.hadoop.fs.FileSystem.listStatus(FileSystem.java:1910)
    at org.apache.hadoop.fs.ChecksumFileSystem.listStatus(ChecksumFileSystem.java:678)
    at org.apache.hadoop.fs.Globber.listStatus(Globber.java:77)
    at org.apache.hadoop.fs.Globber.doGlob(Globber.java:235)
    at org.apache.hadoop.fs.Globber.glob(Globber.java:149)
    at org.apache.hadoop.fs.FileSystem.globStatus(FileSystem.java:2016)

## Solution

We neeed to install hadoop on Windows.

### Prerequisites:

Before you start installing Hadoop on Windows, there are a few prerequisites that you need to have in place:

* Java Development Kit (JDK) version 11 or higher

## Download Hadoop

To install Hadoop on Windows, start by downloading the compatible distribution from the Apache Hadoop website (https://hadoop.apache.org/releases.html). Select the version that matches your Windows system and arch (such as hadoop-3.3.6-aarch64) and download the binary. After the download, extract the contents of hadoop-3.3.6-aarch64.tar.gz:

    C:\ > tar -xf hadoop-3.3.6-aarch64.tar.gz

Ensure to run the terminal as admin to being able to create all hadoop artifacts.

## Set Environment Variables

To use Java & Hadoop, you’ll need to set up some environment variables. This will allow you to run Java & Hadoop commands from any directory on your computer. To set up the environment variables, follow these steps:

1. Open the Start menu and search for “Environment Variables”.
2. Click on “Edit the system environment variables”.
3. Click on the “Environment Variables” button.
4. Under “System Variables”, click on “New”.
5. Enter “JAVA_HOME” as the variable name & the path to the directory where your java is installed (example- C:\Program Files\Java\jdk1.8.0) as the variable value.
6. Click “OK”.
7. Enter “HADOOP_HOME” as the variable name and the path to the directory where you extracted the Hadoop distribution (example- C:\hadoop) as the variable value.
8. Click “OK”.
9. Locate the “Path” variable in the “System Variables” list and click “Edit”.
10. Add the following to the end of the “Variable value” field: %JAVA_HOME%\bin; %HADOOP_HOME%\bin; %HADOOP_HOME%\sbin;
11. Click “OK” to close all the windows.

## Install Hadoop Winutils

Clone or download the winutils repository (https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.6/bin) and copy the contents of hadoop-3.3.6/bin into the extracted location of the Hadoop binary package
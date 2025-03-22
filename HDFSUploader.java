package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import java.io.File;
import java.io.IOException;
import java.net.URI;

public class HDFSUploader {
    public static void main(String[] args) {
        // Local directory containing Yelp JSON files
        //String localDir = "/Users/omnankar/Documents/Spring sem/CSE 587/CSE587project/Yelp JSON/yelp_dataset/";
        String localDir = "/tmp/yelp_dataset/";
        String hdfsDestination = "/yelp_dataset/";

        try {
            // Load Hadoop Configuration
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://namenode:8020"); // Or use the actual Namenode IP
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");

            // Initialize HDFS FileSystem
            FileSystem fs = FileSystem.get(new URI("hdfs://namenode:8020"), conf);

            // Ensure destination directory exists
            Path hdfsDestPath = new Path(hdfsDestination);
            if (!fs.exists(hdfsDestPath)) {
                fs.mkdirs(hdfsDestPath);
                System.out.println("Created HDFS directory: " + hdfsDestination);
            }

            // Get list of files to upload
            File folder = new File(localDir);
            File[] listOfFiles = folder.listFiles((dir, name) -> name.endsWith(".json"));

            if (listOfFiles == null || listOfFiles.length == 0) {
                System.out.println("No JSON files found in local directory: " + localDir);
                return;
            }

            // Upload each file
            for (File file : listOfFiles) {
                Path srcPath = new Path(file.getAbsolutePath());
                Path destPath = new Path(hdfsDestination + file.getName());

                // Check if file already exists in HDFS
                if (fs.exists(destPath)) {
                    System.out.println("Skipping (already exists): " + file.getName());
                    continue;
                }

                // Upload file
                fs.copyFromLocalFile(srcPath, destPath);
                System.out.println("Uploaded: " + file.getName());
            }

            fs.close();
            System.out.println("All files uploaded successfully!");

        } catch (Exception e) {
            System.err.println("Error uploading files to HDFS: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

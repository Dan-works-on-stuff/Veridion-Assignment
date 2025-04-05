package org.example;

import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_HOME", "/dev/null");

        String filePath = "src/main/resources/veridion_entity_resolution_challenge.snappy.parquet";
        // In Main.java, add this check
        File file = new File(filePath);
        if (!file.exists()) {
            throw new IOException("File not found: " + filePath);
        }

        try {
            List<GenericRecord> records = ParquetProcessor.readParquetFile(filePath);
            ParquetProcessor.displayRecords(records);
        } catch (IOException e) {
            System.err.println("Error reading Parquet file:");
            e.printStackTrace();
        }
    }
}
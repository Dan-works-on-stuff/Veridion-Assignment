package org.example;

import org.apache.avro.generic.GenericRecord;
import org.example.ParquetProcessor;

import java.io.File;
import java.io.IOException;
import java.util.List;

// In Main.java
public class Main {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_HOME", "/dev/null");

        String inputPath = "src/main/resources/veridion_entity_resolution_challenge.snappy.parquet";
        String outputPath = "sorted_unique_first.snappy.parquet";

        // Validate input file
        File inputFile = new File(inputPath);
        if (!inputFile.exists()) {
            throw new IOException("Input file not found: " + inputPath);
        }

        // Handle output file
        File outputFile = new File(outputPath);
        if (outputFile.exists() && !outputFile.delete()) {
            throw new IOException("Failed to clear previous output file");
        }

        try {
            List<GenericRecord> records = ParquetProcessor.readParquetFile(inputPath);
            List<GenericRecord> sortedRecords = ParquetProcessor.sortRecordsWithUniqueFirst(records);
            ParquetProcessor.writeParquetFile(sortedRecords, outputPath);

            System.out.println("Successfully created sorted file:");
            System.out.println("- Total records: " + sortedRecords.size());
            System.out.println("- Unique companies: " + (sortedRecords.size() - records.size() + ParquetProcessor.getUniqueCount(sortedRecords)));
            System.out.println("- Output path: " + outputPath);

        } catch (IOException e) {
            System.err.println("Processing failed:");
            e.printStackTrace();
        }
    }
}
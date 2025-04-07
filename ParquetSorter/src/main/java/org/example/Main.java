package org.example;

import org.apache.avro.generic.GenericRecord;
import java.io.File;
import java.io.IOException;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_HOME", "/dev/null");

        String inputPath = "src/main/resources/veridion_entity_resolution_challenge.snappy.parquet";
        String outputPath = "src/main/resources/sorted_companies.snappy.parquet";

        // Validate input file exists
        File inputFile = new File(inputPath);
        if (!inputFile.exists()) {
            throw new IOException("Input file not found: " + inputPath);
        }

        // Handle output file cleanup
        File outputFile = new File(outputPath);
        if (outputFile.exists()) {
            if (!outputFile.delete()) {
                throw new IOException("Failed to delete existing output file: " + outputPath);
            }
            System.out.println("Removed previous output file: " + outputPath);
        }

        try {
            // Read records from input file
            List<GenericRecord> records = ParquetProcessor.readParquetFile(inputPath);

            // Sort records by company name and completeness
            records.sort(ParquetProcessor.getCompanySortingComparator());

            // Write sorted records to new file
            ParquetProcessor.writeParquetFile(records, outputPath);

            System.out.println("\nSuccessfully created sorted file with:");
            System.out.println("- " + records.size() + " records");
            System.out.println("- Sorted by company name (ascending)");
            System.out.println("- Fullest records first for same company names");
            System.out.println("- Output path: " + outputPath);

        } catch (IOException e) {
            System.err.println("Error processing Parquet file:");
            e.printStackTrace();
        }
    }
}
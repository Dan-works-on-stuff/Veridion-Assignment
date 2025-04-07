package org.example;

import org.apache.avro.generic.GenericRecord;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_HOME", "/dev/null");

        // Get input path from command line or prompt
        String inputPath;
        if (args.length > 0) {
            inputPath = args[0];
        } else {
            Scanner scanner = new Scanner(System.in);
            System.out.print("Please enter the input file path: ");
            inputPath = scanner.nextLine();
            scanner.close();
        }

        String uniqueOutputPath = "ParquetSorter/src/main/resources/outputs/unique.snappy.parquet";
        String duplicatesOutputPath = "ParquetSorter/src/main/resources/outputs/duplicates.snappy.parquet";

        // Validate input file
        File inputFile = new File(inputPath);
        if (!inputFile.exists()) {
            throw new IOException("Input file not found: " + inputPath);
        }

        // Handle output files cleanup
        deleteIfExists(uniqueOutputPath);
        deleteIfExists(duplicatesOutputPath);

        try {
            List<GenericRecord> records = ParquetProcessor.readParquetFile(inputPath);
            ParquetProcessor.SplitResult result = ParquetProcessor.splitIntoUniqueAndDuplicates(records);

            // Write unique records
            ParquetProcessor.writeParquetFile(result.getUniqueRecords(), uniqueOutputPath);

            // Write duplicates (non-unique + unnamed)
            ParquetProcessor.writeParquetFile(result.getDuplicateRecords(), duplicatesOutputPath);

            System.out.println("Processing complete:");
            System.out.println("Unique companies: " + result.getUniqueRecords().size());
            System.out.println("Duplicate records: " + result.getDuplicateRecords().size());
            System.out.println("Output files:");
            System.out.println("- " + uniqueOutputPath);
            System.out.println("- " + duplicatesOutputPath);

        } catch (IOException e) {
            System.err.println("Error processing files:");
            e.printStackTrace();
        }
    }

    private static void deleteIfExists(String filePath) throws IOException {
        File file = new File(filePath);
        if (file.exists() && !file.delete()) {
            throw new IOException("Failed to delete existing file: " + filePath);
        }
    }
}
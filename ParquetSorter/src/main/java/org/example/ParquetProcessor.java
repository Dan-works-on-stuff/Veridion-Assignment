package org.example;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ParquetProcessor {

    public static List<GenericRecord> readParquetFile(String filePath) throws IOException {
        List<GenericRecord> records = new ArrayList<>();
        Path path = new Path(filePath);

        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(path).build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                records.add(record);
            }
        }
        return records;
    }

    public static void displayRecords(List<GenericRecord> records) {
        if (records.isEmpty()) {
            System.out.println("No records found");
            return;
        }

        // Print schema
        System.out.println("Schema:");
        System.out.println(records.get(0).getSchema().toString(true));

        // Print records
        System.out.println("\nData:");
        for (GenericRecord record : records) {
            System.out.println(record);
        }
    }
}
package org.example;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
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


    public static void writeParquetFile(List<GenericRecord> records, String outputFilePath) throws IOException {
        if (records == null || records.isEmpty()) {
            throw new IllegalArgumentException("No records to write; the list is empty or null.");
        }

        Schema schema = records.get(0).getSchema();
        Path path = new Path(outputFilePath);

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {
            for (GenericRecord record : records) {
                writer.write(record);
            }
        }
    }

    public static List<GenericRecord> readAndSortByCompanyName(String filePath) throws IOException {
        List<GenericRecord> records = readParquetFile(filePath);
        records.sort(createCompanyComparator());
        return records;
    }

    private static Comparator<? super GenericRecord> createCompanyComparator() {
        return (r1, r2) -> {
            // First: Normalized company name (ascending)
            String name1 = normalizeName(getCompanyName(r1));
            String name2 = normalizeName(getCompanyName(r2));
            int nameCompare = name1.compareTo(name2);
            if (nameCompare != 0) return nameCompare;

            // Second: Completeness score (descending)
            return Integer.compare(calculateCompleteness(r2), calculateCompleteness(r1));
        };
    }

    private static String getCompanyName(GenericRecord record) {
        try {
            Object name = record.get("company_name");
            return name != null ? name.toString() : "";
        } catch (AvroRuntimeException e) {
            return "";
        }
    }

    private static String normalizeName(String rawName) {
        return rawName.toLowerCase().trim().replaceAll("[^a-z0-9]", "");
    }

    private static int calculateCompleteness(GenericRecord record) {
        int populated = 0;
        for (Schema.Field field : record.getSchema().getFields()) {
            if (record.get(field.name()) != null) populated++;
        }
        return populated;
    }

    public static Comparator<GenericRecord> getCompanySortingComparator() {
        return (record1, record2) -> {
            // Compare normalized company names first
            String name1 = normalizeCompanyName(record1);
            String name2 = normalizeCompanyName(record2);
            int nameComparison = name1.compareTo(name2);
            if (nameComparison != 0) return nameComparison;

            // For same names, compare completeness (descending order)
            return Integer.compare(
                    calculateRecordCompleteness(record2),
                    calculateRecordCompleteness(record1)
            );
        };
    }

    private static String normalizeCompanyName(GenericRecord record) {
        Object name = record.get("company_name");
        if (name == null) return "";
        return name.toString()
                .toLowerCase()
                .replaceAll("[^a-z0-9]", "")
                .trim();
    }

    private static int calculateRecordCompleteness(GenericRecord record) {
        int populatedFields = 0;
        for (Schema.Field field : record.getSchema().getFields()) {
            if (record.get(field.name()) != null) {
                populatedFields++;
            }
        }
        return populatedFields;
    }
}
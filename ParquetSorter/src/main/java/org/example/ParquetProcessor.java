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
import java.util.*;
import java.util.stream.Collectors;

public class ParquetProcessor {

    // Core I/O operations -----------------------------------------------------

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

    public static void writeParquetFile(List<GenericRecord> records, String outputFilePath) throws IOException {
        if (records == null || records.isEmpty()) {
            throw new IllegalArgumentException("No records to write");
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

    // Data processing ----------------------------------------------------------

    public static List<GenericRecord> readAndSortByCompanyName(String filePath) throws IOException {
        List<GenericRecord> records = readParquetFile(filePath);
        records.sort(createCompanyComparator());
        return records;
    }

    private static Comparator<GenericRecord> createCompanyComparator() {
        return Comparator
                .comparing(ParquetProcessor::normalizeCompanyName)
                .thenComparing(Comparator.comparingInt(ParquetProcessor::calculateCompleteness).reversed());
    }

    // Core business logic ------------------------------------------------------

    public static List<GenericRecord> sortRecordsWithUniqueFirst(List<GenericRecord> records) {
        Map<String, List<GenericRecord>> groups = groupRecordsByNormalizedName(records);

        List<GenericRecord> uniqueList = new ArrayList<>();
        List<GenericRecord> duplicates = new ArrayList<>();
        List<GenericRecord> unnamed = new ArrayList<>();

        processGroups(groups, uniqueList, duplicates, unnamed);

        return combineResults(uniqueList, duplicates, unnamed);
    }

    // Helper methods ----------------------------------------------------------

    private static Map<String, List<GenericRecord>> groupRecordsByNormalizedName(List<GenericRecord> records) {
        return records.stream().collect(
                Collectors.groupingBy(ParquetProcessor::normalizeCompanyName)
        );
    }

    private static void processGroups(Map<String, List<GenericRecord>> groups,
                                      List<GenericRecord> uniqueList,
                                      List<GenericRecord> duplicates,
                                      List<GenericRecord> unnamed) {
        for (Map.Entry<String, List<GenericRecord>> entry : groups.entrySet()) {
            String normalizedName = entry.getKey();
            List<GenericRecord> group = entry.getValue();

            if (normalizedName.isEmpty()) {
                unnamed.addAll(group);
                continue;
            }

            processNamedGroup(group, uniqueList, duplicates);
        }
    }

    private static void processNamedGroup(List<GenericRecord> group,
                                          List<GenericRecord> uniqueList,
                                          List<GenericRecord> duplicates) {
        int maxCompleteness = group.stream()
                .mapToInt(ParquetProcessor::calculateCompleteness)
                .max().orElse(0);

        GenericRecord bestRecord = group.stream()
                .filter(r -> calculateCompleteness(r) == maxCompleteness)
                .findFirst().orElse(null);

        if (bestRecord != null) {
            uniqueList.add(bestRecord);
            duplicates.addAll(group.stream()
                    .filter(r -> r != bestRecord)
                    .toList());
        }
    }

    private static List<GenericRecord> combineResults(List<GenericRecord> uniqueList,
                                                      List<GenericRecord> duplicates,
                                                      List<GenericRecord> unnamed) {
        List<GenericRecord> result = new ArrayList<>();
        uniqueList.sort(Comparator.comparing(ParquetProcessor::normalizeCompanyName));
        duplicates.sort(createCompanyComparator());
        unnamed.sort(Comparator.comparingInt(ParquetProcessor::calculateCompleteness).reversed());

        result.addAll(uniqueList);
        result.addAll(duplicates);
        result.addAll(unnamed);
        return result;
    }

    // Data quality metrics ----------------------------------------------------

    private static int calculateCompleteness(GenericRecord record) {
        int populated = 0;
        for (Schema.Field field : record.getSchema().getFields()) {
            if (record.get(field.name()) != null) populated++;
        }
        return populated;
    }

    // Normalization utilities --------------------------------------------------

    private static String normalizeCompanyName(GenericRecord record) {
        try {
            Object name = record.get("company_name");
            return name != null ? normalizeString(name.toString()) : "";
        } catch (AvroRuntimeException e) {
            return "";
        }
    }

    private static String normalizeString(String input) {
        return input.toLowerCase()
                .trim()
                .replaceAll("[^a-z0-9]", "");  // Remove special characters for comparison
    }

    // Data structure for result splitting ---------------------------------------

    public static class SplitResult {
        private final List<GenericRecord> uniqueRecords;
        private final List<GenericRecord> duplicateRecords;

        public SplitResult(List<GenericRecord> uniqueRecords, List<GenericRecord> duplicateRecords) {
            this.uniqueRecords = Collections.unmodifiableList(uniqueRecords);
            this.duplicateRecords = Collections.unmodifiableList(duplicateRecords);
        }

        public List<GenericRecord> getUniqueRecords() {
            return new ArrayList<>(uniqueRecords);
        }

        public List<GenericRecord> getDuplicateRecords() {
            return new ArrayList<>(duplicateRecords);
        }
    }

    public static SplitResult splitIntoUniqueAndDuplicates(List<GenericRecord> records) {
        Map<String, List<GenericRecord>> groups = groupRecordsByNormalizedName(records);

        List<GenericRecord> uniqueList = new ArrayList<>();
        List<GenericRecord> duplicates = new ArrayList<>();
        List<GenericRecord> unnamed = new ArrayList<>();

        processGroups(groups, uniqueList, duplicates, unnamed);

        duplicates.addAll(unnamed);
        return new SplitResult(uniqueList, duplicates);
    }
}
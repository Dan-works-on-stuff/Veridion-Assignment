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

    public static String normalizeCompanyName(GenericRecord record) {
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




    public static List<GenericRecord> sortRecordsWithUniqueFirst(List<GenericRecord> records) {
        Map<String, List<GenericRecord>> groups = new HashMap<>();

        // Group records by normalized company name
        for (GenericRecord record : records) {
            String normalizedName = normalizeCompanyName(record);
            groups.computeIfAbsent(normalizedName, k -> new ArrayList<>()).add(record);
        }

        List<GenericRecord> uniqueList = new ArrayList<>();
        List<GenericRecord> nonUniqueNamed = new ArrayList<>();
        List<GenericRecord> unnamedList = new ArrayList<>();

        // Process each group
        for (Map.Entry<String, List<GenericRecord>> entry : groups.entrySet()) {
            String normalizedName = entry.getKey();
            List<GenericRecord> group = entry.getValue();

            if (normalizedName.isEmpty()) {
                unnamedList.addAll(group);
                continue;
            }

            // Find maximum completeness in group
            int maxCompleteness = group.stream()
                    .mapToInt(ParquetProcessor::calculateRecordCompleteness)
                    .max()
                    .orElse(0);

            // Get first record with max completeness
            GenericRecord uniqueRecord = group.stream()
                    .filter(r -> calculateRecordCompleteness(r) == maxCompleteness)
                    .findFirst()
                    .orElse(null);

            if (uniqueRecord != null) {
                uniqueList.add(uniqueRecord);
                // Add remaining group members to non-unique
                group.stream()
                        .filter(r -> r != uniqueRecord)
                        .forEach(nonUniqueNamed::add);
            }
        }

        // Sort each section
        uniqueList.sort(Comparator.comparing(ParquetProcessor::normalizeCompanyName));
        nonUniqueNamed.sort(getCompanySortingComparator());
        unnamedList.sort(completenessDescComparator());

        // Combine results
        List<GenericRecord> result = new ArrayList<>();
        result.addAll(uniqueList);
        result.addAll(nonUniqueNamed);
        result.addAll(unnamedList);

        return result;
    }

    private static Comparator<GenericRecord> completenessDescComparator() {
        return (r1, r2) -> Integer.compare(
                calculateRecordCompleteness(r2),
                calculateRecordCompleteness(r1)
        );
    }
    public static int getUniqueCount(List<GenericRecord> sortedRecords) {
        Set<String> seenNames = new HashSet<>();
        int count = 0;

        for (GenericRecord record : sortedRecords) {
            String normalized = normalizeCompanyName(record);

            // Only count first occurrence of each name
            if (!normalized.isEmpty() && !seenNames.contains(normalized)) {
                seenNames.add(normalized);
                count++;
            }
        }

        return count;
    }


    public static class SplitResult {
        private final List<GenericRecord> uniqueRecords;
        private final List<GenericRecord> duplicateRecords;

        public SplitResult(List<GenericRecord> uniqueRecords, List<GenericRecord> duplicateRecords) {
            this.uniqueRecords = uniqueRecords;
            this.duplicateRecords = duplicateRecords;
        }

        public List<GenericRecord> getUniqueRecords() {
            return new ArrayList<>(uniqueRecords);
        }

        public List<GenericRecord> getDuplicateRecords() {
            return new ArrayList<>(duplicateRecords);
        }
    }

    public static SplitResult splitIntoUniqueAndDuplicates(List<GenericRecord> records) {
        Map<String, List<GenericRecord>> groups = new HashMap<>();

        // Group records by normalized company name
        for (GenericRecord record : records) {
            String normalizedName = normalizeCompanyName(record);
            groups.computeIfAbsent(normalizedName, k -> new ArrayList<>()).add(record);
        }

        List<GenericRecord> uniqueList = new ArrayList<>();
        List<GenericRecord> nonUniqueNamed = new ArrayList<>();
        List<GenericRecord> unnamedList = new ArrayList<>();

        // Process each group
        for (Map.Entry<String, List<GenericRecord>> entry : groups.entrySet()) {
            String normalizedName = entry.getKey();
            List<GenericRecord> group = entry.getValue();

            if (normalizedName.isEmpty()) {
                unnamedList.addAll(group);
                continue;
            }

            // Find maximum completeness in group
            int maxCompleteness = group.stream()
                    .mapToInt(ParquetProcessor::calculateRecordCompleteness)
                    .max()
                    .orElse(0);

            // Get first record with max completeness
            GenericRecord uniqueRecord = group.stream()
                    .filter(r -> calculateRecordCompleteness(r) == maxCompleteness)
                    .findFirst()
                    .orElse(null);

            if (uniqueRecord != null) {
                uniqueList.add(uniqueRecord);
                // Add remaining group members to non-unique
                group.stream()
                        .filter(r -> r != uniqueRecord)
                        .forEach(nonUniqueNamed::add);
            }
        }

        // Sort sections
        uniqueList.sort(Comparator.comparing(ParquetProcessor::normalizeCompanyName));
        nonUniqueNamed.sort(getCompanySortingComparator());
        unnamedList.sort(completenessDescComparator());

        // Combine duplicates
        List<GenericRecord> duplicateRecords = new ArrayList<>(nonUniqueNamed);
        duplicateRecords.addAll(unnamedList);

        return new SplitResult(uniqueList, duplicateRecords);
    }

}
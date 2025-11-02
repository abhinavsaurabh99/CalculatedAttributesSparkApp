package in.abhinavsaurabh.utils;

import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class DatabaseWriterUtil {

    public static void writeDatasetToTableWithNativeUpsertDistributed (
            Dataset<Row> sourceDF,
            String tableName,
            String jdbcUrl,
            Properties props,
            List<String> columnsToWrite,
            List<String> keyColumns,
            Map<String, String> keyColumnMapping
    ) {

        // Broadcast configuration
        final String dbUrl = jdbcUrl;
        final Properties dbProps = new Properties();
        dbProps.putAll(props);

        final String sqlTemplate = buildUpsertSQL(tableName, columnsToWrite, keyColumns);

        if (sqlTemplate == null) {
            System.out.println("Falling back to append mode for unsupported DB.");
            sourceDF.selectExpr(columnsToWrite.toArray(new String[0]))
                    .write()
                    .mode("append")
                    .jdbc(dbUrl, tableName, dbProps);
            return;
        }

        // Rename DataFrame columns to match table names (based on mapping)
        Dataset<Row> renamedDF = sourceDF;
        for (Map.Entry<String, String> entry : keyColumnMapping.entrySet()) {
            String sourceCol = entry.getKey();
            String dbCol = entry.getValue();
            if (!sourceCol.equals(dbCol) && Arrays.asList(renamedDF.columns()).contains(sourceCol)) {
                renamedDF = renamedDF.withColumnRenamed(sourceCol, dbCol);
            }
        }

        // Select only the relevant columns that exist in table
        List<String> existingCols = new ArrayList<>();

        for (String col : columnsToWrite) {
            if (Arrays.asList(renamedDF.columns()).contains(col)) {
                existingCols.add(col);
            } else {
                System.out.printf("Skipping non-existent column '%s', not found in DataFrame %n", col);
            }
        }

        Dataset<Row> writeDF = renamedDF.selectExpr(existingCols.toArray(new String[0]));

        // Main distributed UPSERT logic
        writeDF.foreachPartition((ForeachPartitionFunction<Row>) partition -> {

            if (!partition.hasNext()) return;

            try (Connection conn = DriverManager.getConnection(dbUrl, dbProps);
                 PreparedStatement pstmt = conn.prepareStatement(sqlTemplate)) {

                conn.setAutoCommit(false);
                int batchSize = 0;

                while (partition.hasNext()) {
                    Row row = partition.next();
                    for (int i = 0; i < existingCols.size(); i++) {
                        pstmt.setObject(i + 1, row.get(i));
                    }
                    pstmt.addBatch();
                    if (++batchSize % 5000 == 0) {
                        pstmt.executeBatch();
                        conn.commit();
                    }
                }

                pstmt.executeBatch();
                conn.commit();

            } catch (SQLException e) {
                throw new RuntimeException("Error during UPSERT to " + tableName + ": " + e.getMessage(), e);
            }
        });

        System.out.printf("Distributed UPSERT completed for table %s%n", tableName);
    }

    private static String buildUpsertSQL(String tableName, List<String> cols, List<String> keys) {

        String colList = String.join(", ", cols);
        String placeholders = cols.stream().map(c -> "?").collect(Collectors.joining(", "));
        String keyCols = String.join(", ", keys);

        String updates = cols.stream()
                .filter(c -> !keys.contains(c))
                .map(c -> c + " = EXCLUDED." + c)
                .collect(Collectors.joining(", "));

        return String.format("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
                tableName, colList, placeholders, keyCols, updates);
    }
}

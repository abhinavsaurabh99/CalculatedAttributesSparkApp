package in.abhinavsaurabh.utils;

import org.apache.spark.sql.*;
import org.apache.spark.sql.functions;

import java.util.*;

import static org.apache.spark.sql.functions.*;

/**
 * Utility for dynamically joining multiple Spark tables with optional pivots.
 */
public class DynamicTablesJoinerUtil {

    public static Dataset<Row> dynamicJoin(
            Dataset<Row> baseDF,
            List<TableConfig> otherTables,
            String joinColumn
    ) {

        Dataset<Row> resultDF = baseDF;

        for (TableConfig cfg : otherTables) {
            Dataset<Row> dfToJoin = cfg.getDataset();
            dfToJoin = applyPivotIfRequired(cfg, dfToJoin);

            List<String> joinCols = cfg.getJoinColumns() != null && !cfg.getJoinColumns().isEmpty()
                    ? cfg.getJoinColumns()
                    : List.of("default");

            Column joinCondition = null;

            for (String colName : joinCols) {
                Column condition = resultDF.col(joinColumn).equalTo(dfToJoin.col(colName));
                joinCondition = (joinCondition == null) ? condition : joinCondition.and(condition);
            }

            String joinType = (cfg.getJoinType() != null) ? cfg.getJoinType() : "inner";
            resultDF = resultDF.join(dfToJoin, joinCondition, joinType);
        }

        return resultDF;
    }

    /**
     * Handle pivot logic safely in Java.
     */
    private static Dataset<Row> applyPivotIfRequired(TableConfig cfg, Dataset<Row> dfToJoin) {

        if (cfg == null || dfToJoin == null) {
            throw new IllegalArgumentException("Config or dataset cannot be null");
        }

        String pivotCol = cfg.getPivotColumn();
        String valueCol = cfg.getValueColumn();
        List<String> groupCols = cfg.getGroupByColumns();

        // Only pivot if pivotCol and valueCol are provided
        if (pivotCol != null && !pivotCol.isEmpty() && valueCol != null && !valueCol.isEmpty()) {
            if (groupCols == null || groupCols.isEmpty()) {
                throw new IllegalArgumentException("groupByColumns cannot be empty for pivot table: " + pivotCol);
            }
            String[] groupArr = groupCols.toArray(new String[0]);

            Column aggCol = cfg.getAggregationFunc() != null
                    ? cfg.getAggregationFunc()
                    : first(col(valueCol));

            // Use the correct Java varargs version of groupBy
            if (groupArr.length == 1) {
                dfToJoin = dfToJoin.groupBy(groupArr[0])
                        .pivot(pivotCol)
                        .agg(aggCol);
            } else {
                dfToJoin = dfToJoin.groupBy(
                                groupArr[0],
                                Arrays.copyOfRange(groupArr, 1, groupArr.length)
                        )
                        .pivot(pivotCol)
                        .agg(aggCol);
            }
        }

        return dfToJoin;
    }

    /**
     * Configuration class representing how to process and join a table.
     */
    public static class TableConfig {

        private Dataset<Row> dataset;
        private List<String> groupByColumns;
        private String pivotColumn;
        private String valueColumn;
        private Column aggregationFunc;
        private List<String> joinColumns;
        private String joinType = "inner"; // default join type

        public Dataset<Row> getDataset() { return dataset; }
        public void setDataset(Dataset<Row> dataset) { this.dataset = dataset; }

        public List<String> getGroupByColumns() { return groupByColumns; }
        public void setGroupByColumns(List<String> groupByColumns) { this.groupByColumns = groupByColumns; }

        public String getPivotColumn() { return pivotColumn; }
        public void setPivotColumn(String pivotColumn) { this.pivotColumn = pivotColumn; }

        public String getValueColumn() { return valueColumn; }
        public void setValueColumn(String valueColumn) { this.valueColumn = valueColumn; }

        public Column getAggregationFunc() { return aggregationFunc; }
        public void setAggregationFunc(Column aggregationFunc) { this.aggregationFunc = aggregationFunc; }

        public List<String> getJoinColumns() { return joinColumns; }
        public void setJoinColumns(List<String> joinColumns) { this.joinColumns = joinColumns; }

        public String getJoinType() { return joinType; }
        public void setJoinType(String joinType) { this.joinType = joinType; }
    }

    /**
     * Dynamically updates specific columns in a small dataset from a large dataset,
     * even when both have different schemas.
     */
    public static Dataset<Row> updateSmallFromLarge(
            Dataset<Row> smallDF,
            Dataset<Row> largeDF,
            List<String> joinColumns,
            List<String> updateColumns
    ) {

        // Broadcast the small dataset for efficiency
        Dataset<Row> broadcastSmall = functions.broadcast(smallDF);

        // Build join condition dynamically
        Column joinCondition = joinColumns.stream()
                .map(c -> broadcastSmall.col(c).equalTo(largeDF.col(c)))
                .reduce(Column::and)
                .orElseThrow(() -> new IllegalArgumentException("Join columns required"));

        // Perform LEFT join (to retain all rows from smallDF)
        Dataset<Row> joined = broadcastSmall.join(largeDF, joinCondition, "left");

        // Update only matching columns from largeDF
        for (String colName : updateColumns) {
            if (largeDF.columns().length > 0 && hasColumn(largeDF, colName)) {
                joined = joined.withColumn(colName, coalesce(largeDF.col(colName), broadcastSmall.col(colName)));
            } else {
                System.out.println("Skipping column " + colName + " (not found in largeDF)");
            }
        }

        // Drop duplicate join columns from largeDF if any
        for (String jc : joinColumns) {
            if (hasColumn(joined, jc + "_1")) {
                joined = joined.drop(jc + "_1");
            }
        }

        // Return updated dataset with only smallDF columns
        return joined.selectExpr(smallDF.columns());
    }

    // Helper to safely check if column exists
    private static boolean hasColumn(Dataset<Row> df, String colName) {

        for (String c : df.columns()) {
            if (c.equalsIgnoreCase(colName)) return true;
        }
        return false;
    }
}

package in.abhinavsaurabh.services.implementations;

import java.util.*;
import java.util.stream.Collectors;

import in.abhinavsaurabh.models.FormulaModel;
import in.abhinavsaurabh.services.IFormulaService;
import in.abhinavsaurabh.utils.FormulaParserUtil;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static in.abhinavsaurabh.constants.DatabaseConstant.*;

import static org.apache.spark.sql.functions.*;

public class FormulaServiceImplV1 implements IFormulaService {

    private final SparkSession spark;
    private static final Logger logger = LoggerFactory.getLogger(FormulaServiceImplV1.class);

    public FormulaServiceImplV1(SparkSession spark) {
        this.spark = spark;
    }

    /**
     * Load items from database using JDBC
     */
    public Dataset<Row> loadItemsFromDb(
            String jdbcUrl,
            String tableName,
            Map<String, String> columnAliasMap,
            Properties connectionProps,
            String partitionColumn,
            long lowerBound,
            long upperBound,
            int numPartitions
    ) {

        String selectClause = columnAliasMap.entrySet().stream()
                .map(e -> e.getKey() + " AS " + e.getValue())
                .collect(Collectors.joining(", "));

        String query = String.format("SELECT %s FROM %s", selectClause, tableName);
        String cleanAlias = tableName.replace(".", "_");
        String aliasedQuery = "(" + query + ") AS " + cleanAlias + "_alias";

        return spark.read()
                .option("partitionColumn", partitionColumn)
                .option("lowerBound", lowerBound)
                .option("upperBound", upperBound)
                .option("numPartitions", numPartitions)
                .jdbc(jdbcUrl, aliasedQuery, connectionProps);
    }

    /**
     * Build the WHEN-THEN expressions for each field
     */
    private Map<String, Column> buildChainedWhen(Map<Long, Map<String, Column>> formulaMap, Dataset<Row> df) {

        Map<String, Column> updatedColumns = new HashMap<>();

        // Collect all fields across all categories dynamically
        Set<String> allFields = new HashSet<>();
        for (Map<String, Column> fieldMap : formulaMap.values()) {
            allFields.addAll(fieldMap.keySet());
        }

        // For each field (description, my_part_number, etc.)
        for (String field : allFields) {
            Column formulaExpr = null;

            // Build chained when for each category
            for (Map.Entry<Long, Map<String, Column>> outerEntry : formulaMap.entrySet()) {
                Long categoryID = outerEntry.getKey();
                Map<String, Column> innerFormulas = outerEntry.getValue();

                Column formula = innerFormulas.get(field);
                if (formula == null) continue;

                //Column currentExpr = expr(formula);

                if (formulaExpr == null) {
                    formulaExpr = when(col(CATEGORY_ID).equalTo(lit(categoryID)), formula);
                } else {
                    formulaExpr = formulaExpr.when(col(CATEGORY_ID).equalTo(lit(categoryID)), formula);
                }
            }

            // Default to existing field value if no category matched
            if (formulaExpr != null) {
                formulaExpr = formulaExpr.otherwise(col(field));
                updatedColumns.put(field, formulaExpr);
            }
        }

        return updatedColumns;
    }

    /**
     * Apply withColumn transformations and return updated DataFrame
     */
    public Dataset<Row> applyFormulas(Dataset<Row> df, Map<Long, Map<String, String>> formulaMap) {
        Map<Long, Map<String, Column>> sparkFormulaMap = this.convertToSparkNativeExpression(formulaMap);
        Map<String, Column> updatedColumns = buildChainedWhen(sparkFormulaMap, df);
        Dataset<Row> resultDF = df;
        for (Map.Entry<String, Column> entry : updatedColumns.entrySet()) {
            resultDF = resultDF.withColumn(entry.getKey(), entry.getValue());
        }
        return resultDF;
    }

    public Map<Long, Map<String, Column>> convertToSparkNativeExpression(Map<Long, Map<String, String>> formulaMap) {

        Map<Long, Map<String, Column>> sparkExprMap = new HashMap<>();

        for (Map.Entry<Long, Map<String, String>> outerEntry : formulaMap.entrySet()) {
            Long categoryId = outerEntry.getKey();
            Map<String, String> fieldMap = outerEntry.getValue();

            Map<String, Column> fieldExprMap = new HashMap<>();

            for (Map.Entry<String, String> fieldEntry : fieldMap.entrySet()) {
                String fieldName = fieldEntry.getKey();
                String token = fieldEntry.getValue();

                if (token == null || token.isEmpty()) continue;

                // Build the expression from tokens
                Column expression = buildSparkExpression(token);
                fieldExprMap.put(fieldName, expression);
            }

            sparkExprMap.put(categoryId, fieldExprMap);
        }

        return sparkExprMap;
    }

    // Helper to convert tokens to Spark-native expression strings
    public static Column buildSparkExpression(String expression) {

        String[] parts = expression.split("&");
        List<Column> columns = new ArrayList<>();
        for (String part : parts) {
            part = part.trim();
            if (part.startsWith("[") && part.endsWith("]")) {
                String fieldName = part.substring(1, part.length() - 1);
                if (fieldName.startsWith("a_")) {
                    // Remove a_ prefix when accessing pivoted column
                    String actualField = fieldName.substring(2);
                    columns.add(col(actualField));
                } else {
                    columns.add(col(fieldName));
                }
            } else if (part.startsWith("\"") && part.endsWith("\"")) {
                columns.add(lit(part.substring(1, part.length() - 1)));
            } else if (!part.isEmpty()) {
                columns.add(lit(part));
            }
        }
        System.out.println(concat_ws("", columns.toArray(new Column[0])));
        return concat_ws("", columns.toArray(new Column[0]));
    }

    public Map<Long, Map<String, String>> getFormulaMap(SparkSession spark) {

        Dataset<Row> formulaDf = spark.read()
                .format("jdbc")
                .option("url", "jdbc:postgresql://localhost:5432/postgres")
                .option("dbtable", "( " +
                        "SELECT a.category_id, c.field_name, b.formula_definition " +
                        "FROM category_field a " +
                        "INNER JOIN formula b ON a.id = b.category_field_id " +
                        "INNER JOIN lu_field_name c ON c.id = b.field_id " +
                        ") AS formula_data")
                .option("user", "postgres")
                .option("password", "postgres")
                .load();

        /*Dataset<Row> formulaDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:postgresql://localhost:5432/postgres")
                .option("dbtable", "( " +
                        "SELECT a.category_id, c.field_name, b.formula_definition " +
                        "FROM category_field a " +
                        "INNER JOIN formula b ON a.id = b.category_field_id " +
                        "INNER JOIN lu_field_name c ON c.id = b.field_id " +
                        ") AS formula_data")
                .option("user", "postgres")
                .option("password", "postgres")
                .option("partitionColumn", "category_id")  // must appear in SELECT
                .option("lowerBound", 1)
                .option("upperBound", 10000)
                .option("numPartitions", 8)
                .load();*/

        Dataset<FormulaModel> formulaDataset = formulaDf.map(

                (MapFunction<Row, FormulaModel>) row -> {

                    Long categoryId = row.getAs(CATEGORY_ID);
                    String fieldName = row.getAs(FIELD_NAME);
                    String formula = row.getAs(FORMULA_DEFINITION);

                    boolean isFormulaValid;
                    try {
                        FormulaParserUtil.parseFormula(formula);
                        isFormulaValid = true;
                    } catch (Exception e) {
                        isFormulaValid = false;
                    }

                    return new FormulaModel(categoryId, fieldName, formula);
                },
                Encoders.bean(FormulaModel.class)
        );

        Map<Long, Map<String, String>> formulaMap = new HashMap<>();

        for (FormulaModel formulaModel : formulaDataset.collectAsList()) {
            String token = formulaModel.getFormula() == null || formulaModel.getFormula().isEmpty()
                    ? ""
                    : formulaModel.getFormula();

            formulaMap.computeIfAbsent(formulaModel.getCategoryId(), k -> new HashMap<>())
                    .put(formulaModel.getFieldName(), token);
        }

        logger.info("Final formula map: {}", formulaMap);
        return formulaMap;
    }
}

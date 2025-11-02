package in.abhinavsaurabh.services;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;
import java.util.Properties;

public interface IFormulaService {

    Dataset<Row> loadItemsFromDb(
            String jdbcUrl,
            String tableName,
            Map<String, String> columnAliasMap,
            Properties connectionProps,
            String partitionColumn,
            long lowerBound,
            long upperBound,
            int numPartitions
    );

    Map<Long, Map<String, String>> getFormulaMap(SparkSession spark);

    Dataset<Row> applyFormulas(Dataset<Row> df, Map<Long, Map<String, String>> formulaMap);
}

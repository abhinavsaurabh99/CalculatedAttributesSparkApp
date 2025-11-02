package in.abhinavsaurabh.processors;

import in.abhinavsaurabh.services.IFormulaService;
import in.abhinavsaurabh.services.implementations.FormulaServiceImplV1;
import in.abhinavsaurabh.utils.DatabaseWriterUtil;
import in.abhinavsaurabh.utils.DynamicTablesJoinerUtil;

import org.apache.spark.sql.*;

import java.util.*;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.first;

public class CalculatedAttributesProcessor {

    private final SparkSession spark;
    private final IFormulaService formulaService;

    public CalculatedAttributesProcessor() {

        this.spark = SparkSession.builder()
                .appName("FormulaBuilder")
                .master("local[*]")
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate();

        this.formulaService = new FormulaServiceImplV1(spark);
    }

    public void process() {

        String jdbcUrl = "jdbc:postgresql://localhost:5432/postgres";
        Properties props = new Properties();
        props.put("user", "postgres");
        props.put("password", "postgres");
        props.put("driver", "org.postgresql.Driver");

        Dataset<Row> itemDf = this.formulaService.loadItemsFromDb(
                jdbcUrl,
                "prime.item",
                this.getItemAliasMap("item"),
                props,
                "item_id",
                1L,
                1000000L,
                10
        );

        Dataset<Row> itemCategoryDf = this.formulaService.loadItemsFromDb(
                jdbcUrl,
                "prime.item_category",
                this.getItemAliasMap("itemcategory"),
                props,
                "item_category_id",
                1L,
                1000000L,
                10
        );

        Dataset<Row> itemAttributeDf = this.formulaService.loadItemsFromDb(
                jdbcUrl,
                "prime.item_attribute_value",
                this.getItemAliasMap("itemattribute"),
                props,
                "item_attribute_id",
                1L,
                1000000L,
                10
        );

        Dataset<Row> attributeDf = this.formulaService.loadItemsFromDb(
                jdbcUrl,
                "prime.attribute",
                this.getItemAliasMap("attribute"),
                props,
                "attribute_id",
                1L,
                1000000L,
                10
        );

        Dataset<Row> itemCustomFieldDf = this.formulaService.loadItemsFromDb(
                jdbcUrl,
                "prime.item_custom_field_value",
                this.getItemAliasMap("itemcustomfield"),
                props,
                "item_custom_field_id",
                1L,
                1000000L,
                10
        );

        Dataset<Row> customFieldDf = this.formulaService.loadItemsFromDb(
                jdbcUrl,
                "prime.custom_field",
                this.getItemAliasMap("customfield"),
                props,
                "custom_field_id",
                1L,
                1000000L,
                10
        );

        Dataset<Row> itemCategoryJoinedDf = this.buildItemCategoryDataset(itemDf, itemCategoryDf);
        itemCategoryJoinedDf = itemCategoryJoinedDf.drop(col("item_id"));
        Dataset<Row> attributeItemAttributeJoinedDf = this.buildAttributeDataset(attributeDf, itemAttributeDf);
        Dataset<Row> customFieldItemCustomFieldJoinedDf = this.buildCustomFieldDataset(customFieldDf, itemCustomFieldDf);

        Dataset<Row> itemJoinedDf = this.buildItemDataset(
                itemCategoryJoinedDf,
                attributeItemAttributeJoinedDf,
                customFieldItemCustomFieldJoinedDf
        );

        Map<Long, Map<String, String>> formulaMap = this.formulaService.getFormulaMap(spark);
        Dataset<Row> result = this.formulaService.applyFormulas(itemJoinedDf, formulaMap);

        Map<String, String> aliasMap = this.getItemAliasMap("item");
        aliasMap.remove("enriched_indicator");
        aliasMap.remove("manufacturer_status");
        aliasMap.remove("status");
        aliasMap.put("id", "category_item_id");
        Map<String, String> revMap = new HashMap<>();
        for (Map.Entry<String, String> entry : aliasMap.entrySet()) {
            revMap.put(entry.getValue(), entry.getKey());
        }

        DatabaseWriterUtil.writeDatasetToTableWithNativeUpsertDistributed(
                result,
                "prime.item",
                jdbcUrl,
                props,
                aliasMap.keySet().stream().toList(),
                List.of("id"),
                revMap
        );
    }

    private Dataset<Row> buildItemCategoryDataset(Dataset<Row> left, Dataset<Row> right)
    {

        DynamicTablesJoinerUtil.TableConfig categoryConfig = new DynamicTablesJoinerUtil.TableConfig();
        categoryConfig.setDataset(right);
        categoryConfig.setJoinColumns(List.of("category_item_id"));
        categoryConfig.setJoinType("inner");

        return DynamicTablesJoinerUtil.dynamicJoin(left, List.of(categoryConfig), "item_id");
    }

    private Dataset<Row> buildAttributeDataset(Dataset<Row> left, Dataset<Row> right)
    {

        DynamicTablesJoinerUtil.TableConfig attributeConfig = new DynamicTablesJoinerUtil.TableConfig();
        attributeConfig.setDataset(right);
        attributeConfig.setJoinColumns(List.of("attribute_id"));
        attributeConfig.setJoinType("inner");

        return DynamicTablesJoinerUtil.dynamicJoin(left, List.of(attributeConfig), "attribute_id");
    }

    private Dataset<Row> buildCustomFieldDataset(Dataset<Row> left, Dataset<Row> right)
    {

        DynamicTablesJoinerUtil.TableConfig customFieldConfig = new DynamicTablesJoinerUtil.TableConfig();
        customFieldConfig.setDataset(right);
        customFieldConfig.setJoinColumns(List.of("custom_field_id"));
        customFieldConfig.setJoinType("inner");

        return DynamicTablesJoinerUtil.dynamicJoin(left, List.of(customFieldConfig), "custom_field_id");
    }

    private Dataset<Row> buildItemDataset(Dataset<Row> itemDf,
                                          Dataset<Row> attributeDf,
                                          Dataset<Row> customFieldDf)
    {

        DynamicTablesJoinerUtil.TableConfig attrConfig = new DynamicTablesJoinerUtil.TableConfig();
        attrConfig.setDataset(attributeDf);
        attrConfig.setJoinColumns(List.of("attribute_item_id"));
        attrConfig.setGroupByColumns(List.of("attribute_item_id"));
        attrConfig.setPivotColumn("attribute_name");
        attrConfig.setValueColumn("attribute_value");
        attrConfig.setJoinType("left");
        attrConfig.setAggregationFunc(first(col("attribute_value")));

        Dataset<Row> df = DynamicTablesJoinerUtil.dynamicJoin(itemDf, List.of(attrConfig), "category_item_id");

        DynamicTablesJoinerUtil.TableConfig customFieldConfig = new DynamicTablesJoinerUtil.TableConfig();
        customFieldConfig.setDataset(customFieldDf);
        customFieldConfig.setJoinColumns(List.of("custom_field_item_id"));
        customFieldConfig.setGroupByColumns(List.of("custom_field_item_id"));
        customFieldConfig.setPivotColumn("custom_field_name");
        customFieldConfig.setValueColumn("custom_field_value");
        customFieldConfig.setJoinType("left");
        customFieldConfig.setAggregationFunc(first(col("custom_field_value")));

        return DynamicTablesJoinerUtil.dynamicJoin(df, List.of(customFieldConfig), "category_item_id");
    }

    private Map<String, String> getItemAliasMap(String option) {

        Map<String, String> aliasMap = new HashMap<>();

        if ("item".equals(option)) {
            aliasMap.put("id", "item_id");
            aliasMap.put("part_number", "part_number");
            aliasMap.put("brand_name", "brand_name");
            aliasMap.put("mfr_part_number", "mfr_part_number");
            aliasMap.put("my_part_number", "my_part_number");
            aliasMap.put("upc", "upc");
            aliasMap.put("country_of_origin", "country_of_origin");
            aliasMap.put("country_of_sale", "country_of_sale");
            aliasMap.put("gtin", "gtin");
            aliasMap.put("short_description", "short_description");
            aliasMap.put("long_description", "long_description");
            aliasMap.put("feature_bullets", "feature_bullets");
            aliasMap.put("invoice_description", "invoice_description");
            aliasMap.put("qty_available", "qty_available");
            aliasMap.put("package_length", "package_length");
            aliasMap.put("package_height", "package_height");
            aliasMap.put("package_weight", "package_weight");
            aliasMap.put("package_width", "package_width");
            aliasMap.put("volume", "volume");
            aliasMap.put("standard_approval", "standard_approval");
            aliasMap.put("status", "status");
            aliasMap.put("warranty", "warranty");
            aliasMap.put("enriched_indicator", "enriched_indicator");
            aliasMap.put("manufacturer_status", "manufacturer_status");
        } else if ("itemcategory".equals(option)) {
            aliasMap.put("id", "item_category_id");
            aliasMap.put("category_id", "category_id");
            aliasMap.put("item_id", "category_item_id");
        } else if ("customfield".equals(option)) {
            aliasMap.put("id", "custom_field_id");
            aliasMap.put("field_name", "custom_field_name");
        } else if ("itemcustomfield".equals(option)) {
            aliasMap.put("id", "item_custom_field_id");
            aliasMap.put("custom_field_value", "custom_field_value");
            aliasMap.put("custom_field_id", "custom_field_id");
            aliasMap.put("item_id", "custom_field_item_id");
        } else if ("attribute".equals(option)) {
            aliasMap.put("id", "attribute_id");
            aliasMap.put("attribute_name", "attribute_name");
        } else if ("itemattribute".equals(option)) {
            aliasMap.put("id", "item_attribute_id");
            aliasMap.put("attribute_value", "attribute_value");
            aliasMap.put("attribute_id", "attribute_id");
            aliasMap.put("item_id", "attribute_item_id");
        }
        return aliasMap;
    }
}

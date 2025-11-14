###### Calculated Attributes Spark Service

## Overview
- This project implements a **high-performance distributed data processing system** using **Apache Spark** for 
  large-scale computation of dynamic item attributes based on category-specific formula.

- It fetches the category_ids, the item_fields (for which formula is created) and their corresponding formula in a `HashMap<Long, HashMap<String, String>>`
  from multiple tables through runtime joins(`category_field` table, `lu_field_name` table, 'formula` table) and parses the raw symbolic formula first
  through a custom lexical parser to validate the syntax and then parses it again into a meaningful Spark expression.
  
  | Category ID             | Field Name     | Formula                                  |
  | ----------------------- | -------------- | ---------------------------------------- |
  | **10001 (Electronics)** | full_name      | concat(brand, ' ', model)                |
  |                         | discount_price | price - (price * discount_percent / 100) |
  |                         | description    | concat(short_desc, ' ', long_desc)       |
  | **10002 (Furniture)**   | full_name      | concat(material, ' ', type)              |
  |                         | discount_price | price - discount_amount                  |
  ---------------------------------------------------------------------------------------

- Then it reads data from a relational database from multiple tables through dynamic runtime distributed joins(`items` table, `category` table, 
  `attribute` table, `custom_field` table), computes formula-based field values (such as `short_desc`), 
  and writes the computed results back efficiently 
  
- capable of processing **100+ million records** with excellent scalability and throughput.

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------

## Core Features
- **Dynamic Formula Evaluation** — Fetch and apply category-specific formulas dynamically using Spark SQL expressions.  
- **Distributed Computation** — Execute transformations in parallel using Spark’s distributed engine.  
- **High-Throughput Write-Back** — Batch JDBC operations for efficient database writes.  
- **Scalable Architecture** — Seamlessly scales from thousands to hundreds of millions of records.  
- **Extensible Design** — Add new formulas or fields through metadata without code changes.

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------

## System Architecture
**1. Input Source:** Relational database with:
- `items` table (base data)
- `category_formula` table (category-wise formula metadata)

**2. Computation Engine:** Apache Spark cluster (Structured API) handling distributed processing.

**3. Output Target:** Same database — updated with computed item fields.

**4. Key Components:**
- **Formula Loader:** Fetches category-wise formula definitions.  
- **Expression Generator:** Converts formulas into Spark SQL expressions.  
- **Computation Executor:** Executes transformations on distributed data.  
- **DBWriterUtil:** Writes computed results back using batched JDBC connections.

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------

## Performance & Scalability
| Parameter | Configuration | Outcome |
|------------|----------------|----------|
| **Dataset Size**    | 100 million records                 | Efficient distributed processing |
| **Cluster Size**    | 10–50 nodes                         | Near-linear performance scaling  |
| **Throughput**      | >5 million records/minute (tunable) | High-throughput computation      |
| **Write Mode**      | Batched JDBC append                 | Reduced DB contention            |
| **Fault Tolerance** | Spark retries & checkpoints         | Minimal data loss risk           |

## Leveraging Spark’s **lazy evaluation**, **predicate pushdown**, and **in-memory execution**, the system achieves optimal speed and resource utilization.

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------

## Example Workflow
**1. Load formulas from database:** 
- val formulaMap = loadFormulasFromDB()

**2. Build Spark SQL expressions for selected columns:** 
- val transformedDF = sourceDF.selectExpr(columnsToWrite: _*)

**3. Write computed values back to the table:** 
- transformedDF.write.mode("append").jdbc(dbUrl, tableName, dbProps)
	
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Tech Stack
| Layer              | Technology       |
| ------------------ | ---------------- |
| Computation        | **Apache Spark** |
| Language           | **Java**         |
| Database           | **PostgreSQL**   |
| Data Access        | **JDBC**         |
| Database Migration | **Liquibase**    |
| Build Tool         | **Gradle**       |
-----------------------------------------


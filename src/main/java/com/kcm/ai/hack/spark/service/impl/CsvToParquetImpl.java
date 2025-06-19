package com.kcm.ai.hack.spark.service.impl;

import com.kcm.ai.hack.spark.service.CsvToParquetService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class CsvToParquetImpl implements CsvToParquetService {
    @Override
    public List<String> csvToParquet() {
        try {
            // Initialize SparkSession
            SparkSession spark = SparkSession.builder()
                    .appName("CSV to Parquet")
                    .master("local[*]")  // local mode
                    .config("spark.ui.enabled", "false")
                    .config("spark.hadoop.hadoop.native.lib", "false")
                    .getOrCreate();

            // Step 1: Read CSV file
            Dataset<Row> csvData = spark.read()
                    .option("header", "true") // If CSV has headers
                    .option("inferSchema", "true") // Automatically infer data types
                    .csv("D:\\Tanusri\\blogs\\parquet\\LambdaTest-random-generator.csv"); // Replace with your path

            // Step 2: Write to Parquet
            csvData.write()
                    .mode("overwrite") // Overwrite if exists
                    .parquet("D:\\Tanusri\\blogs\\parquet\\output_parquet");

            System.out.println("CSV successfully converted to Parquet.");

            Dataset<Row> df = spark.read().parquet("D:\\Tanusri\\blogs\\parquet\\output_parquet");

            df.show();
            df.printSchema();
            // Convert to JSON string
            List<String> jsonList = df.toJSON().collectAsList();
            spark.stop();
            return jsonList;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> csvToParquetUsingApacheIceberge() {
        try {
            SparkSession spark = SparkSession.builder()
                    .appName("IcebergIntegration")
                    .master("local[*]")
                    .config("spark.ui.enabled", "false")
                    .config("spark.hadoop.hadoop.native.lib", "false")
                    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
                    .config("spark.sql.catalog.my_catalog.type", "hadoop")
                    .config("spark.sql.catalog.my_catalog.warehouse", "file:///C:/iceberg/warehouse")
                    .getOrCreate();

            // Create a table
            spark.sql("CREATE TABLE IF NOT EXISTS my_catalog.db.users (id INT, name STRING) USING iceberg");

            // Insert data
            spark.sql("INSERT INTO my_catalog.db.users VALUES (1, 'Amit'), (2, 'Bansal')");

            // Query
            Dataset<Row> df = spark.sql("SELECT * FROM my_catalog.db.users");
            df.show();

            df.printSchema();
            // Convert to JSON string
            List<String> jsonList = df.toJSON().collectAsList();
            spark.stop();
            return jsonList;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

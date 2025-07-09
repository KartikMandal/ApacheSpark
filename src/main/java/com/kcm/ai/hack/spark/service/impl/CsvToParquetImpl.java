package com.kcm.ai.hack.spark.service.impl;

import com.kcm.ai.hack.spark.request.UserQueryRequest;
import com.kcm.ai.hack.spark.service.CsvToParquetService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Service;

import java.io.File;
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


    @Override
    public List<String> csvToParquetWithFile(File file) {
        try {
            String parquetOutputPath = "D:\\Tanusri\\blogs\\parquet\\output_parquet";

            SparkSession spark = SparkSession.builder()
                    .appName("CSV File to Parquet")
                    .master("local[*]")
                    .config("spark.ui.enabled", "false")
                    .config("spark.hadoop.hadoop.native.lib", "false")
                    .getOrCreate();

            // Step 1: Read CSV file
            Dataset<Row> csvData = spark.read()
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv(file.getAbsolutePath());

            // Step 2: Write to Parquet
            csvData.write()
                    .mode(SaveMode.Overwrite)
                    .parquet(parquetOutputPath);

            System.out.println("CSV successfully converted to Parquet at: " + parquetOutputPath);

            // Step 3: Read the Parquet and return as JSON
            Dataset<Row> df = spark.read().parquet(parquetOutputPath);

            df.show(false); // Disable truncation for wider data
            df.printSchema();

            List<String> jsonList = df.toJSON().collectAsList();
            spark.stop();
            return jsonList;

        } catch (Exception e) {
            throw new RuntimeException("Error during CSV to Parquet conversion: " + e.getMessage(), e);
        }
    }

    @Override
    public List<String> csvToParquetUsingApacheIcebergeWithVariable(List<UserQueryRequest> userList) {
        try {
            SparkSession spark = SparkSession.builder()
                    .appName("IcebergIntegration")
                    .master("local[*]")
                    .config("spark.ui.enabled", "false")
                    .config("spark.hadoop.hadoop.native.lib", "false")
                    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
                    .config("spark.sql.catalog.my_catalog.type", "hadoop")
                    .config("spark.sql.catalog.my_catalog.warehouse", "file:///C:/iceberg/warehouse2")
                    .getOrCreate();

            // Create table if not exists
            spark.sql("CREATE TABLE IF NOT EXISTS my_catalog.db.users (id INT, name STRING) USING iceberg");

            // Convert Java list to Spark DataFrame
            Dataset<Row> userDF = spark.createDataFrame(userList, UserQueryRequest.class);
            userDF.createOrReplaceTempView("temp_users");

            // Insert into Iceberg table
            spark.sql("INSERT INTO my_catalog.db.users SELECT id, name FROM temp_users");

            // Query for verification
            Dataset<Row> df = spark.sql("SELECT * FROM my_catalog.db.users");
            df.show();
            df.printSchema();

            // Return as JSON
            List<String> jsonList = df.toJSON().collectAsList();
            spark.stop();
            return jsonList;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

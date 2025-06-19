package com.kcm.ai.hack.spark.service;

import java.util.List;

public interface CsvToParquetService {
    List<String> csvToParquet();

    List<String> csvToParquetUsingApacheIceberge();
}

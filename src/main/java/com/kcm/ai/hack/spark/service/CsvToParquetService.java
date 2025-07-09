package com.kcm.ai.hack.spark.service;

import com.kcm.ai.hack.spark.request.UserQueryRequest;

import java.io.File;
import java.util.List;

public interface CsvToParquetService {
    List<String> csvToParquet();

    List<String> csvToParquetUsingApacheIceberge();

    List<String> csvToParquetWithFile(File file);

    List<String> csvToParquetUsingApacheIcebergeWithVariable(List<UserQueryRequest> userList);
}

package com.kcm.ai.hack.spark.controller;

import com.kcm.ai.hack.spark.request.UserQueryRequest;
import com.kcm.ai.hack.spark.service.CsvToParquetService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.util.List;

@RestController
@RequestMapping("/apache")
public class ApacheSparkController {

    @Autowired
    private CsvToParquetService csvToParquetService;

    @PostMapping("/spark")
    public ResponseEntity<String> handleFileQuery() {
        try {
            List<String> jsonList = csvToParquetService.csvToParquet(); // Extract from JSON
            // Combine as a JSON array
            String jsonArray = "[" + String.join(",", jsonList) + "]";
            return ResponseEntity.ok()
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(jsonArray);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("{\"error\": \"" + e.getMessage().replace("\"", "'") + "\"}");
        }
    }

    @PostMapping("/spark-iceberg")
    public ResponseEntity<String> handleFileIcebergQuery() {
        try {
            List<String> jsonList = csvToParquetService.csvToParquetUsingApacheIceberge(); // Extract from JSON
            // Combine as a JSON array
            String jsonArray = "[" + String.join(",", jsonList) + "]";
            return ResponseEntity.ok()
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(jsonArray);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("{\"error\": \"" + e.getMessage().replace("\"", "'") + "\"}");
        }
    }


    @PostMapping(value = "/spark-file", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<String> csvToParquetWithFile(@RequestParam("file") MultipartFile file) {
        try {
            File tempFile = File.createTempFile("upload", ".csv");
            file.transferTo(tempFile);

            List<String> jsonList = csvToParquetService.csvToParquetWithFile(tempFile);
            // Combine as a JSON array
            String jsonArray = "[" + String.join(",", jsonList) + "]";
            return ResponseEntity.ok()
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(jsonArray);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("{\"error\": \"" + e.getMessage().replace("\"", "'") + "\"}");
        }
    }

    @PostMapping("/spark-iceberg-obj")
    public ResponseEntity<String> csvToParquetUsingApacheIcebergeWithVariable(@RequestBody List<UserQueryRequest> userList) {
        try {
            List<String> jsonList = csvToParquetService.csvToParquetUsingApacheIcebergeWithVariable(userList); // Extract from JSON
            // Combine as a JSON array
            String jsonArray = "[" + String.join(",", jsonList) + "]";
            return ResponseEntity.ok()
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(jsonArray);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("{\"error\": \"" + e.getMessage().replace("\"", "'") + "\"}");
        }
    }


}


package com.example.tablefile.controller;

import com.example.tablefile.model.Query;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.*;

@RestController
public class ReqController {


    @PostMapping("/addUpdate")
    public String addUpdate(@RequestBody Query query) throws IOException {

        String matadata = "matadata.txt";
        String textFile = "textFile.txt";


        System.out.println(query);

        String queryString = query.getQuery();

        StringBuilder stringBuilder = new StringBuilder(queryString);

        String firstLetter = stringBuilder.substring(0, 1);

        if (firstLetter.equalsIgnoreCase("C")) {

            String cacheKey = "createTable:" + queryString;
            String cachedResponse = redisTemplate.opsForValue().get(cacheKey);
            if (cachedResponse != null) {


                File file = new File("matadata.txt");
                boolean fileExist = true;

                if (!file.exists()) {
                    file.createNewFile();
                    fileExist = false;
                }

                try (FileWriter fileWriter = new FileWriter(matadata)) {

                    String header1 = "dataType";
                    String header2 = "FieldName";

                    fileWriter.write("+---------------+---------------+\n");

                    // Write the headers to the file
                    fileWriter.write(String.format("| %-13s | %-13s |%n", header1, header2));

                    int startIndexOfBreaket = stringBuilder.indexOf("(");
                    int endIndexOfBreaket = stringBuilder.indexOf(")");

                    String subStringOfQuery = stringBuilder.substring(startIndexOfBreaket, endIndexOfBreaket);
                    System.out.println(subStringOfQuery);

                    subStringOfQuery = subStringOfQuery.replaceAll("[()]", "");

                    String[] subStringArray = subStringOfQuery.split(",");

                    for (String substring : subStringArray) {
                        System.out.println(substring.trim());

                        String FieldName = substring.substring(0, substring.lastIndexOf(" ")).trim();
                        String dataType = substring.substring(substring.lastIndexOf(" "), substring.length()).trim();

                        fileWriter.write("+---------------+---------------+\n");

                        fileWriter.write(String.format("| %-13s | %-13s |%n", dataType, FieldName));

                        System.out.println("Table created successfully!");

                    }
                    fileWriter.write("+---------------+---------------+\n");

                    String successResponse = "Table created successfully!";
                    redisTemplate.opsForValue().set(cacheKey, successResponse);

                    // Save the SQL statement and success status in Redis
                    saveSqlStatementToRedis(queryString, true);

                    return successResponse;

                } catch (IOException e) {
                    e.printStackTrace();

                    String failResponse = "An error occurred while creating the table.";
                    redisTemplate.opsForValue().set(cacheKey, failResponse);

                    // Save the SQL statement and failure status in Redis
                    saveSqlStatementToRedis(queryString, false);

                    return failResponse;
                }
//
            }
            return cachedResponse;
//===================******======================**********================*********================************==================***************************
//===================******======================**********================*********================************==================***************************
//===================******======================**********================*********================************==================***************************
// ===================******======================**********================*********================************==================***************************


        } else if (firstLetter.equalsIgnoreCase("I")) {


            String cacheKey = "insertData:" + queryString;
            String cachedResponse = redisTemplate.opsForValue().get(cacheKey);
            if (cachedResponse != null) {


                int lineCount = 0;
                File file = new File("textFile.txt");
                File fileMetadata = new File("matadata.txt");
                boolean fileExist = true;

                if (!fileMetadata.exists()) {
                    return " Table not creted!, So, you can not insert a Data ;";
                }

                if (!file.exists()) {
                    file.createNewFile();
                    fileExist = false;
                }
                try (BufferedWriter fileWriter1 = new BufferedWriter(new FileWriter(textFile, true))) {


                    try (BufferedReader reader = new BufferedReader(new FileReader(matadata))) {
                        while (reader.readLine() != null) {
                            lineCount++;
                        }
                    } catch (IOException e) {
                        System.out.println("An error occurred while reading the file.");
                        e.printStackTrace();
                    }

                    lineCount = lineCount - (lineCount / 2) - 2;

                    int startIndexOfBreaketForCol = stringBuilder.indexOf("(") + 1;
                    int endIndexOfBreaketForCol = stringBuilder.indexOf(")");

                    String subStringOfQuery = stringBuilder.substring(startIndexOfBreaketForCol, endIndexOfBreaketForCol);

                    String[] subStringArrayIN = subStringOfQuery.split(",");

                    int lengthOfArray = subStringArrayIN.length;

                    if (lineCount != lengthOfArray) {
                        return " There is some issue in your Query, please correct your Query for Insert Data ;";
                    }
                    if (!fileExist) {
                        for (int i = 0; i < lengthOfArray; i++) {
                            fileWriter1.write("+---------------+");
                        }
                        fileWriter1.write("\n");

                        for (String substring : subStringArrayIN) {

                            substring = substring.trim();
                            fileWriter1.write(String.format("| %-13s  ", substring));

                        }
                        fileWriter1.write("|");
                        fileWriter1.write("\n");
                        for (int i = 0; i < lengthOfArray; i++) {
                            fileWriter1.write("+---------------+");
                        }
                        fileWriter1.write("\n");
                        fileExist = true;
                    }
                    stringBuilder = stringBuilder.delete(0, endIndexOfBreaketForCol + 1);
                    stringBuilder = stringBuilder.delete(0, stringBuilder.indexOf("(") + 1);

                    String[] subStringArrayForVal = stringBuilder.toString().split("\\(");
                    String[] valueArray;
                    for (String substringVal : subStringArrayForVal) {
                        System.out.println(substringVal.trim());

                        substringVal = substringVal.replaceAll("\\)", "");
                        substringVal = substringVal.trim();
                        valueArray = substringVal.split(",");

                        if (!fileExist) {

                            for (int i = 0; i < lengthOfArray; i++) {
                                fileWriter1.write("+---------------+");
                            }
                            fileWriter1.write("\n");
                        }
                        fileExist = false;

//                        if (!firstRow) {
//                            fileWriter1.write("\n");
//                        } else {
//                            firstRow = false;
//                        }

                        for (String finalVal : valueArray) {
                            System.out.println(finalVal);

                            fileWriter1.write(String.format("| %-13s  ", finalVal));
                        }
                        fileWriter1.write("|");
                        fileWriter1.write("\n");
                    }

                    String successResponse = "Data inserted successfully!";
                    redisTemplate.opsForValue().set(cacheKey, successResponse);

                    // Save the SQL statement and success status in Redis
                    saveSqlStatementToRedis(queryString, true);

//                    f () i

                    for (int i = 0; i < lengthOfArray; i++) {
                        fileWriter1.write("+---------------+");
                    }
                    fileWriter1.write("\n");
                    return successResponse;

                } catch (IOException e) {
                    e.printStackTrace();

                    String failResponse = "An error occurred while inserting the data.";
                    redisTemplate.opsForValue().set(cacheKey, failResponse);

                    // Save the SQL statement and failure status in Redis
                    saveSqlStatementToRedis(queryString, false);

                    return failResponse;
                }

            }
            return cachedResponse;
        } else {

            String failResponse = "An error occurred while fetching a query.";
            redisTemplate.opsForValue().set(query.getQuery(), failResponse);

            // Save the SQL statement and failure status in Redis
            saveSqlStatementToRedis(queryString, false);

            return failResponse;

        }

    }

    @Autowired
    private RedisTemplate<String, String> redisTemplate;


    private void saveSqlStatementToRedis(String queryString, boolean success) {
        // Generate a unique key for the SQL statement
        String sqlKey = "sqlStatement:" + queryString;

        // Save the SQL statement in Redis
        redisTemplate.opsForValue().set(sqlKey, queryString);

        // Save the success/failure status in Redis
        String statusKey = "sqlStatus:" + queryString;
        String statusValue = success ? "success" : "failure";
        redisTemplate.opsForValue().set(statusKey, statusValue);
    }
}

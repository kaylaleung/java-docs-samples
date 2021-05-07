/*
 * Copyright 2017 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.spanner;

// Imports the Google Cloud client library
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.WriteBuilder;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.jdbc.CloudSpannerJdbcConnection;
import com.google.cloud.storage.BlobInfo;
import com.google.common.collect.Iterables;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import java.nio.file.Paths;

/** Sample showing how to load CSV file data into Spanner */
public class PostgresHackathon {

  public static final String EXCEL = "EXCEL";
  public static final String POSTGRESQL_CSV = "POSTGRESQL_CSV";
  public static final String POSTGRESQL_TEXT = "POSTGRESQL_TEXT}";

  enum SpannerDataTypes {
    INT64,
    FLOAT64,
    BOOL,
    DATE,
    TIMESTAMP,
    STRING
  }

  public static ArrayList<SpannerDataTypes> spannerSchema = new ArrayList<>();
  public static Connection connection;

  /** Return the data type of the column type **/
  public static SpannerDataTypes parseSpannerDataType(String columnType) {
    if (columnType.matches("STRING(?:\\((?:MAX|[0-9]+)\\))?")) {
      return SpannerDataTypes.STRING;
    } else if (columnType.equalsIgnoreCase("INT64")) {
      return SpannerDataTypes.INT64;
    } else if (columnType.equalsIgnoreCase("FLOAT64")) {
      return SpannerDataTypes.FLOAT64;
    } else if (columnType.equalsIgnoreCase("BOOL")) {
      return SpannerDataTypes.BOOL;
    } else if (columnType.equalsIgnoreCase("DATE")) {
      return SpannerDataTypes.DATE;
    } else if (columnType.equalsIgnoreCase("TIMESTAMP")) {
      return SpannerDataTypes.TIMESTAMP;
    } else {
      throw new IllegalArgumentException(
          "Unrecognized or unsupported column data type: " + columnType);
    }
  }

  /** Initialize CSV Parser format based on user specified option flags **/
  public static CSVFormat setFormat(CommandLine cmd) {
    CSVFormat parseFormat;
    // Set file format type
    if (cmd.hasOption("f")) {
      switch (cmd.getOptionValue("f").toUpperCase()) {
        case EXCEL:
          parseFormat = CSVFormat.EXCEL;
          break;
        case POSTGRESQL_CSV:
          parseFormat = CSVFormat.POSTGRESQL_CSV;
          break;
        case POSTGRESQL_TEXT:
          parseFormat = CSVFormat.POSTGRESQL_TEXT;
          break;
        default:
          parseFormat = CSVFormat.DEFAULT;
      }
    } else {
      parseFormat = CSVFormat.DEFAULT;
    }
    // Set null string representation
    if (cmd.hasOption("n")) {
      parseFormat = parseFormat.withNullString(cmd.getOptionValue("n"));
    }
    // Set delimiter character
    if (cmd.hasOption("d")) {
      if (cmd.getOptionValue("d").length() != 1) {
        throw new IllegalArgumentException("Invalid delimiter character entered.");
      }
      parseFormat = parseFormat.withDelimiter(cmd.getOptionValue("d").charAt(0));
    }
    // Set escape character
    if (cmd.hasOption("e")) {
      if (cmd.getOptionValue("e").length() != 1) {
        throw new IllegalArgumentException("Invalid escape character entered.");
      }
      parseFormat = parseFormat.withEscape(cmd.getOptionValue("e").charAt(0));
    }
    // Set parser to parse first row as headers
    if (cmd.hasOption("h") && cmd.getOptionValue("h").equalsIgnoreCase("True")) {
      parseFormat = parseFormat.withFirstRecordAsHeader();
    }
    return parseFormat;
  }


  /** Write CSV file data to Spanner using JDBC Mutation API **/
  public static void writeToSpanner(CSVParser parser, String tableName, CommandLine cmd)
      throws SQLException {
    System.out.println("Writing data into table...");

    // NOTE: Headers are currently required to be set
    List<String> headers = parser.getHeaderNames();
    // for (String header : headers) {
    //   System.out.println(header);
    // }

    Iterable<CSVRecord> records = parser;
    CSVRecord dataTypes = Iterables.get(records, 0);
    for (int i = 0; i < dataTypes.size(); i++) {
      // System.out.println(dataTypes.get(i));
      spannerSchema.add(parseSpannerDataType(dataTypes.get(i)));
    }

    List<Mutation> mutations = new ArrayList<>();
    for (CSVRecord record : records) {
      WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(tableName);
      for (int i = 0; i < headers.size(); i++) {
        if (record.get(i) != null) {
          switch (spannerSchema.get(i)) {
            case BOOL:
              builder.set(headers.get(i)).to(Boolean.parseBoolean(record.get(i)));
              break;
            case INT64:
              builder.set(headers.get(i)).to(Integer.parseInt(record.get(i).trim()));
              break;
            case FLOAT64:
              builder.set(headers.get(i)).to(Float.parseFloat(record.get(i).trim()));
              break;
            case STRING:
              builder.set(headers.get(i)).to(record.get(i).trim());
              break;
            case DATE:
              builder.set(headers.get(i))
                  .to(com.google.cloud.Date.parseDate(record.get(i).trim()));
              break;
            case TIMESTAMP:
              builder.set(headers.get(i))
                  .to(com.google.cloud.Timestamp.parseTimestamp(record.get(i)));
              break;
            default:
              System.err.print("Invalid Type. This type is not supported.");
          }
        }
      }
      mutations.add(builder.build());
    }
    CloudSpannerJdbcConnection spannerConnection = connection
        .unwrap(CloudSpannerJdbcConnection.class);
    spannerConnection.write(mutations);
    spannerConnection.close();
    System.out.println("Data successfully written into table.");
  }

  public static void downloadObject(
      String projectId, String bucketName, String objectName, String destFilePath) {

    Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
    Blob blob = storage.get(BlobId.of(bucketName, objectName));
    blob.downloadTo(Paths.get(destFilePath));
    System.out.println(
        "Downloaded object "
            + objectName
            + " from bucket name "
            + bucketName
            + " to "
            + destFilePath);
  }

  public static void uploadObject(
      String projectId, String bucketName, String objectName, String filePath) throws IOException {

    Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
    BlobId blobId = BlobId.of(bucketName, objectName);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    storage.create(blobInfo, Files.readAllBytes(Paths.get(filePath)));
    System.out.println(
        "File " + filePath + " uploaded to bucket " + bucketName + " as " + objectName);
  }

  public static void main(String... args) throws Exception {

    // Initialize option flags
    Options opt = new Options();
    opt.addOption("h", true, "File Contains Header");
    opt.addOption("f", true, "Format Type of Input File");
    opt.addOption("n", true, "String Representing Null Value");
    opt.addOption("d", true, "Character Separating Columns");
    opt.addOption("e", true, "Character To Escape");

    opt.addOption("source", true, "File Location Source");
    opt.addOption("action", true, "Specify COPY action");
    opt.addOption("bucket", true, "Bucket Name");
    opt.addOption("object", true, "Object Name Name");

    CommandLineParser clParser = new DefaultParser();
    CommandLine cmd = clParser.parse(opt, args);

    if (args.length < 4) {
      System.out.println("LoadCsvExample <instance_id> <database_id> <table_id> <path_to_csv>");
      return;
    }

    SpannerOptions options = SpannerOptions.newBuilder().build();
    Spanner spanner = options.getService();
    String projectId = "span-cloud-testing";

    String instanceId = args[0];
    String databaseId = args[1];
    String tableName = args[2];
    String filepath = args[3];

    // Download CSV file from Google Cloud Storage and load CSV data into spanner
    if (cmd.getOptionValue("action").equalsIgnoreCase("TO")) {
      if (cmd.getOptionValue("source").equalsIgnoreCase("GCS")) {
        // Download CSV File to Local
        downloadObject(projectId, cmd.getOptionValue("bucket"), cmd.getOptionValue("object"),
            "./" + filepath);
      }
      // Write CSV file data to Cloud Spanner
      try {
        connection = DriverManager.getConnection(
            String.format(
                "jdbc:cloudspanner:/projects/%s/instances/%s/databases/%s",
                projectId, instanceId, databaseId));

        Reader in = new FileReader(filepath);
        CSVFormat parseFormat = setFormat(cmd);
        CSVParser parser = CSVParser.parse(in, parseFormat);

        try {
          writeToSpanner(parser, tableName, cmd);
        } catch (SQLException e) {
          System.out.println(e.getMessage());
        }

      } finally {
        spanner.close();
      }
    }
    else if (cmd.getOptionValue("action").equalsIgnoreCase("FROM")) {
      uploadObject(projectId, cmd.getOptionValue("bucket"), cmd.getOptionValue("object"), "./" + args[3]);
    }
  }
}

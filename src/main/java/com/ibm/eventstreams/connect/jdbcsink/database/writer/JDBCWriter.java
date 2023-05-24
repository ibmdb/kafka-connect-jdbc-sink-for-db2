/*
 *
 * Copyright 2020 IBM Corporation
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
 *
 */

package com.ibm.eventstreams.connect.jdbcsink.database.writer;

import com.ibm.eventstreams.connect.jdbcsink.JDBCSinkTask;
import com.ibm.eventstreams.connect.jdbcsink.database.datasource.IDataSource;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class JDBCWriter implements IDatabaseWriter {

    private static final Logger logger = LoggerFactory.getLogger(JDBCSinkTask.class);

    private final IDataSource dataSource;

    public JDBCWriter(final IDataSource dataSource) {
        this.dataSource = dataSource;
    }

    private boolean doesTableExist(Connection connection, String tableName) throws SQLException {
        String[] tableParts = tableName.split("\\.");
        DatabaseMetaData dbm = connection.getMetaData();
        ResultSet table = dbm.getTables(null, tableParts[0], tableParts[1], null);
        return table.next();
    }

    private boolean isPostgreSQL(Connection connection) throws SQLException {
        return connection.getMetaData().getDatabaseProductName().toLowerCase().contains("postgresql");
    }

    private boolean isDB2(Connection connection) throws SQLException {
        return connection.getMetaData().getDatabaseProductName().toLowerCase().contains("db2");
    }

    private boolean isMySQL(Connection connection) throws SQLException {
        return connection.getMetaData().getDatabaseProductName().toLowerCase().contains("mysql");
    }

    private String getPostgreSQLFieldType(Schema.Type fieldType) {
        // Map Kafka Connect types to PostgreSQL data types
        // Adjust this mapping based on your specific requirements
        switch (fieldType) {
            case INT8:
            case INT16:
            case INT32:
                return "INTEGER";
            case INT64:
                return "BIGINT";
            case FLOAT32:
                return "REAL";
            case FLOAT64:
                return "DOUBLE PRECISION";
            case BOOLEAN:
                return "BOOLEAN";
            case STRING:
                return "VARCHAR(255)";
            default:
                return "VARCHAR(255)";
        }
    }

    private String getDB2FieldType(Schema.Type fieldType) {
        // Map Kafka Connect types to DB2 data types
        // Adjust this mapping based on your specific requirements
        switch (fieldType) {
            case INT8:
            case INT16:
            case INT32:
                return "INTEGER";
            case INT64:
                return "BIGINT";
            case FLOAT32:
                return "REAL";
            case FLOAT64:
                return "DOUBLE";
            case BOOLEAN:
                return "BOOLEAN";
            case STRING:
                return "VARCHAR(255)";
            default:
                return "VARCHAR(255)";
        }
    }

    private String getMySQLFieldType(Schema.Type fieldType) {
        // Map Kafka Connect types to MySQL data types
        // Adjust this mapping based on your specific requirements
        switch (fieldType) {
            case INT8:
            case INT16:
            case INT32:
                return "INT";
            case INT64:
                return "BIGINT";
            case FLOAT32:
                return "FLOAT";
            case FLOAT64:
                return "DOUBLE";
            case BOOLEAN:
                return "BOOLEAN";
            case STRING:
                return "VARCHAR(255)";
            default:
                return "VARCHAR(255)";
        }
    }

    private String getIdColumnDefinition() {
        try {
            if (isPostgreSQL(dataSource.getConnection())) {
                if (getPostgresMajorVersion() <= 9) {
                    return "id SERIAL PRIMARY KEY";
                } else {
                    return "id INTEGER PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY";
                }
            } else if (isDB2(dataSource.getConnection())) {
                return "id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY";
            } else if (isMySQL(dataSource.getConnection())) {
                return "id INTEGER PRIMARY KEY AUTO_INCREMENT";
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return "id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY";
    }

    private int getPostgresMajorVersion() {
        Connection connection;
        try {
            connection = dataSource.getConnection();
            DatabaseMetaData metaData = connection.getMetaData();
            String databaseProductName = metaData.getDatabaseProductName();
            if ("PostgreSQL".equals(databaseProductName)) {
                int majorVersion = metaData.getDatabaseMajorVersion();
                return majorVersion;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public void createTable(Connection connection, String tableName, Schema schema) throws SQLException {
        final String CREATE_STATEMENT = "CREATE TABLE %s (%s)";

        StringBuilder fieldDefinitions = new StringBuilder();
        // fieldDefinitions.append("id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY
        // KEY");
        fieldDefinitions.append(getIdColumnDefinition());

        for (Field field : schema.fields()) {
            String fieldName = field.name();
            Schema.Type fieldType = field.schema().type();
            String nullable = field.schema().isOptional() ? "" : " NOT NULL";

            // Add field definitions based on database-specific data types
            if (isPostgreSQL(connection)) {
                fieldDefinitions
                        .append(String.format(", %s %s%s", fieldName, getPostgreSQLFieldType(fieldType), nullable));
            } else if (isDB2(connection)) {
                fieldDefinitions.append(String.format(", %s %s%s", fieldName, getDB2FieldType(fieldType), nullable));
            } else if (isMySQL(connection)) {
                fieldDefinitions.append(String.format(", %s %s%s", fieldName, getMySQLFieldType(fieldType), nullable));
            } else {
                throw new SQLException("Unsupported database type");
            }
        }

        String createTableSql = String.format(CREATE_STATEMENT, tableName, fieldDefinitions.toString());

        System.out.println("Creating table: " + tableName);
        System.out.println("Field definitions: " + fieldDefinitions.toString());
        System.out.println("Final prepared statement: " + createTableSql);

        try (PreparedStatement pstmt = connection.prepareStatement(createTableSql)) {
            pstmt.execute();
        }

        System.out.println("Table " + tableName + " has been created");
    }

    private String buildInsertStatement(String tableName, List<String> fieldNames) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(tableName);
        sb.append("(").append(String.join(", ", fieldNames)).append(")");
        sb.append(" VALUES ");
        sb.append("(").append(String.join(", ", Collections.nCopies(fieldNames.size(), "?"))).append(")");
        return sb.toString();
    }

    @Override
    public void insert(String tableName, Collection<SinkRecord> records) throws SQLException {
        Connection connection = null;
        try {
            connection = this.dataSource.getConnection();

            if (!doesTableExist(connection, tableName)) {
                logger.info("Table not found. Creating table: " + tableName);
                createTable(connection, tableName, records.iterator().next().valueSchema());
            }

            List<String> fieldNames = records.iterator().next().valueSchema().fields().stream()
                    .map(Field::name)
                    .collect(Collectors.toList());

            String insertStatement = buildInsertStatement(tableName, fieldNames);
            logger.debug("Insert Statement: {}", insertStatement);
            PreparedStatement pstmt = connection.prepareStatement(insertStatement);

            for (SinkRecord record : records) {
                Struct recordValue = (Struct) record.value();

                List<Object> fieldValues = fieldNames.stream()
                        .map(fieldName -> recordValue.get(fieldName))
                        .collect(Collectors.toList());

                for (int i = 0; i < fieldValues.size(); i++) {
                    pstmt.setObject(i + 1, fieldValues.get(i));
                }

                pstmt.addBatch();
                logger.debug("Record added to batch: {}", record.value());
            }

            int[] batchResults = pstmt.executeBatch();
            logger.debug("Batch execution results: {}", Arrays.toString(batchResults));

            pstmt.close();
        } catch (BatchUpdateException batchUpdateException) {
            logger.error("SOME OPERATIONS IN BATCH FAILED");
            logger.error(batchUpdateException.toString());
        } catch (SQLException sQLException) {
            logger.error(sQLException.toString());
            throw sQLException;
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }
}

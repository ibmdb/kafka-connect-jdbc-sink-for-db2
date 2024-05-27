/*
 *
 * Copyright 2020, 2023 IBM Corporation
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

import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialException;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.eventstreams.connect.jdbcsink.JDBCSinkTask;
import com.ibm.eventstreams.connect.jdbcsink.database.builder.CommandBuilder;
import com.ibm.eventstreams.connect.jdbcsink.database.datasource.IDataSource;
import com.ibm.eventstreams.connect.jdbcsink.database.utils.DataSourceFactor;

public class JDBCWriter implements IDatabaseWriter {

    private static final Logger logger = LoggerFactory.getLogger(JDBCSinkTask.class);

    private final DataSourceFactor dataSourceFactor;
    private final CommandBuilder commandBuilder;

    public JDBCWriter(final IDataSource dataSource) {
        this.dataSourceFactor = new DataSourceFactor(dataSource);
        this.commandBuilder = new CommandBuilder();
    }

    public void createTable(String tableName, Schema schema) throws SQLException {
        logger.trace("[{}] Entry {}.createTable, props={}", Thread.currentThread().getId(), this.getClass().getName());

        final String CREATE_STATEMENT = "CREATE TABLE %s (%s)";

        StringBuilder fieldDefinitions = new StringBuilder();
        fieldDefinitions.append(commandBuilder.getIdColumnDefinition(dataSourceFactor));

        for (Field field : schema.fields()) {
            String fieldName = field.name();
            Schema.Type fieldType = field.schema().type();
            String nullable = field.schema().isOptional() ? "" : " NOT NULL";

            // Add field definitions based on database-specific data types
            if (dataSourceFactor.isPostgreSQL()) {
                fieldDefinitions
                        .append(String.format(", %s %s%s", fieldName, commandBuilder.getPostgreSQLFieldType(fieldType),
                                nullable));
            } else if (dataSourceFactor.isDB2()) {
                fieldDefinitions.append(
                        String.format(", %s %s%s", fieldName, commandBuilder.getDB2FieldType(fieldType), nullable));
            } else if (dataSourceFactor.isMySQL()) {
                fieldDefinitions.append(
                        String.format(", %s %s%s", fieldName, commandBuilder.getMySQLFieldType(fieldType), nullable));
            } else {
                throw new SQLException("Unsupported database type");
            }
        }

        String createTableSql = String.format(CREATE_STATEMENT, tableName, fieldDefinitions.toString());

        logger.debug("Creating table: " + tableName);
        logger.debug("Field definitions: " + fieldDefinitions.toString());
        logger.debug("Final prepared statement: " + createTableSql);
        System.out.println("Final prepared statement: " + createTableSql);

        try (PreparedStatement pstmt = dataSourceFactor.prepareStatement(createTableSql)) {
            pstmt.execute();
        }

        logger.info("Table " + tableName + " has been created");
        logger.trace("[{}]  Exit {}.createTable", Thread.currentThread().getId(), this.getClass().getName());
    }

    @Override
    public void insert(String tableName, String insertFunction, Collection<SinkRecord> records) throws SQLException {
        boolean insertMemoryTable = false;

        logger.trace("[{}] Entry {}.insert, props={}", Thread.currentThread().getId(), this.getClass().getName());
        try {
            if (!dataSourceFactor.doesTableExist(tableName)) {
                logger.info("Table not found. Creating table: " + tableName);
                createTable(tableName, records.iterator().next().valueSchema());
            }

            // Use special Db2 MEMORY_TABLE function and set up records to use that function for insert
            try{
                if (insertFunction != null && insertFunction.equalsIgnoreCase("memory_table") && dataSourceFactor.isDB2()) {
                    logger.trace("Insert using DB2 MEMORY_TABLE insert");
                    System.out.println("Insert using DB2 MEMORY_TABLE insert");
                    insertMemoryTable = true;
                } else {
                    logger.trace("Insert using JDBC batch insert");
                    System.out.println("Insert using JDBC batch insert");
                }
            } catch (Exception e) {
                logger.trace("Insert using JDBC batch insert in Exception");
                System.out.println("Insert using JDBC batch insert in Exception");
            }

            
            List<String> fieldNames = records.iterator().next().valueSchema().fields().stream()
                    .map(Field::name)
                    .collect(Collectors.toList());

            if (insertMemoryTable) {
                System.out.println("In MEMORY_TABLE EXECUTION");
                // Instead of the JDBC insert row batch, the Db2 MEMORY_TABLE needs to do the following:
                //      - Get the schema and set up rowsize once, then set up getters. I.e. use setupBatchInsert
                //      - Separate out work of getting the byte buffer < 2GB by going through all rows make bufferlist as in setupBatchInsert
                //      - Format the MEMORY_TABLE insert statement
                //      - For each buffer in the list execute the insert (using batchInsertAsyncNewSendData) and when we get an error stop and return exception
                try {
                    Schema schema = records.iterator().next().valueSchema();
                    List<Triple> buffers = setupBatchInsert(schema, fieldNames, records);
                    System.out.println("Done setupBatchInsert");

                    String sql = formatMemoryTableSQLInsert(tableName, schema, fieldNames);
                    System.out.println("The memory table sql is = " + sql);

                    // Now for each buffer run the sql statement with the buffer using JDBC
                    for (Triple triple : buffers) {
                        batchInsertAsyncNewSendData(sql, triple.getRight(), triple.getMiddle(), triple.getLeft());
                    }
                } catch (Exception e) {
                    logger.trace("Issue on MEMORY_TABLE setup batch");
                    System.out.println("Insert on MEMORY_TABLE setup batch");
                }
            } else {
                System.out.println("In JDBC EXECUTION");

                String insertStatement = commandBuilder.buildInsertStatement(tableName, fieldNames);
                logger.debug("Insert Statement: {}", insertStatement);
                System.out.println("Insert Statement: " + insertStatement);
                PreparedStatement pstmt = dataSourceFactor.prepareStatement(insertStatement);

                for (SinkRecord record : records) {
                    Struct recordValue = (Struct) record.value();

                    List<Object> fieldValues = fieldNames.stream()
                            .map(fieldName -> recordValue.get(fieldName))
                            .collect(Collectors.toList());

                    logger.debug("Field values: {}", fieldValues);
                    for (int i = 0; i < fieldValues.size(); i++) {
                        pstmt.setObject(i + 1, fieldValues.get(i));
                    }

                    pstmt.addBatch();
                    logger.debug("Record added to batch: {}", record.value());
                }

                int[] batchResults = pstmt.executeBatch();
                logger.debug("Batch execution results: {}", Arrays.toString(batchResults));

                pstmt.close();
            }
        } catch (BatchUpdateException batchUpdateException) {
            logger.error("SOME OPERATIONS IN BATCH FAILED");
            logger.error(batchUpdateException.toString());
            throw batchUpdateException;
        } catch (SQLException sqlException) {
            logger.error(sqlException.toString());
            throw sqlException;
        } finally {
            dataSourceFactor.close();
        }
        logger.trace("[{}]  Exit {}.insert", Thread.currentThread().getId(), this.getClass().getName());
    }

    // this returns the number of bytes per data type + null indicator
    private int addToRowSize(Schema.Type fieldType) {
        switch (fieldType) {
            case BOOLEAN:
            case INT8:
            case INT16:
                return 2 + 1;
            case FLOAT32:
            case INT32:
                return 4 + 1;
            case FLOAT64:
            case INT64:
                return 8 + 1;
            case STRING:
                return 255 + 2;
            default:
                return 255 + 2;
        }
    }

    private List<Triple/*<int, int, byte[]>*/> setupBatchInsert(Schema schema, List<String> fieldNames, Collection<SinkRecord> rows) throws SQLException, IOException {
        // this function is responsible for prepping the data to be inserted via batch insert methods (includes async)
        /*
        The MEMORY_TABLE function limits the amount of data that can be inserted (2GB).
        If the batch to be inserted is more than this limit, the batch is split into mini-batches smaller than the limit, and
        inserted separately. The batches are stored in the ListBuffer "buffers", a list of byte arrays.
        More specifically, this function works by evaluating the size of the row to be inserted, if the row size +
        size of current mini-batch (byte array) is less than the limit, the row is added to the current mini-batch.
        If the row size + size of current mini-batch (byte array) is greater than the limit, the mini-batch
        byte array is added to the ListBuffer, and the row is added to a new mini-batch byte array.
        ex. If you have a 4GB batch, this will be split into two 2GB batches in ListBuffer to be inserted asynchronously
        */

        // the limit of the MEMORY_TABLE function
        int memoryTableInsertBufferLimitInGB = 2;

        // the ListBuffer which contains information of: byte batch #, number of rows in batch, the batch itself (in byte array)
        List<Triple/*int, int, byte[]*/> buffers = new ArrayList<Triple/*int, int, byte[]*/>();

        // byte batch #
        int miniBatchNum = 0;

        // calculate the size (in bytes) of each row by adding up the column widths of the schema
        // initialize to 2 to account for byte-order-mark (BOM) which indicates endianess the client operates in
        int rowSize = 2;
        //List[String] typesNamesList = schema.schema.map(s => (s.name, s.dataType));
        
        // loop through each column type, calculate estimated row size
        for (Field field : schema.fields()) {
            Schema.Type fieldType = field.schema().type();
            // From the typoe, get the correct byte size + nullindicator size to total rowSize
            rowSize += addToRowSize(fieldType);
        }
        
        logger.debug("setupBatchInsert rowsize = " + rowSize);

        // if the row itself cannot fit into the memory table byte buffer limit, exit with error
        if (rowSize > memoryTableInsertBufferLimitInGB*1024*1024*1024L) {
             throw new SQLException("$memoryTableInsertBufferLimitInGB GB Memory Table byte buffer limit reached: " +
                    "Row size: $rowSize bytes");
        }

        // variables for putting the rows into bytestream
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            dos.writeShort(0xFEFF);
        } catch (IOException ioException) {
            logger.error(ioException.toString());
            throw ioException;
        }

        int totalRowsInserted = 0;
        int rowsInsertedIntoBaos = 0;
        long totalBytes = 2;

        List<Boolean> nullabilities = new ArrayList<Boolean>();
        List<Schema.Type> fieldTypes = new ArrayList<Schema.Type>();

        for (Field field : schema.fields()) {
            fieldTypes.add(field.schema().type());
            nullabilities.add(field.schema().isOptional());
        }

        /**
        Loop through rows, access appropriate getter function for each column data type.
        Each row is converted into a byte stream.
        Whenever the byte stream reaches the Memory Table byte buffer limit, a "mini batch" is created and added to the
        "buffers" listbuffer as a byte array.
        From here, a new byte stream is created for the remaining rows to be inserted, creating
        new mini batches if necessarry. If no new mini buffer is created by the end, the remaining rows are added to "buffers".
        This continues until all of the rows are inside the "buffers" listbuffer.
        */
        System.out.println/*logger.debug*/("Looping through the batch, adding each row to byte stream");
        for (SinkRecord record : rows) {

            Struct recordValue = (Struct) record.value();

            List<Object> fieldValues = fieldNames.stream()
                    .map(fieldName -> recordValue.get(fieldName))
                    .collect(Collectors.toList());

            logger.debug("Field values: {}", fieldValues);

            logger.debug("Record added to batch: {}", record.value());
   
            // add the row to the byte buffer if (row size + total rows inserted) is less than the size insert limit
            try {
                if (totalBytes + rowSize <= memoryTableInsertBufferLimitInGB*1024*1024*1024L) {
                    System.out.println/*logger.debug*/("Row + existing bytes added < $memoryTableInsertBufferLimitInGB GB, adding row to byte stream");

                    for (int i = 0; i < fieldValues.size(); i++) {
                        makeGetter(dos, fieldTypes.get(i), nullabilities.get(i), fieldValues.get(i));
                    }
                    
                    logger.debug("Record added to batch: {}", record.value());
                    rowsInsertedIntoBaos += 1;
                    totalBytes += rowSize;
                } else {
                    /**
                     The bytesteam is filled to the memory table limit and cannot add the current row.
                        Write the current byte stream to the buffers listbuffer.
                        Then create a new bytestream and add the current row to it
                        */
                    System.out.println/*logger.debug*/("$memoryTableInsertBufferLimitInGB GB limit reached, adding current byte array to list of byte arrays (to be inserted)");
                    miniBatchNum += 1;
                    Triple newtriple = new Triple(miniBatchNum, rowsInsertedIntoBaos, baos.toByteArray());
                    buffers.add(newtriple);

                    // reset the dos/baos
                    baos = new ByteArrayOutputStream();
                    dos = new DataOutputStream(baos);
                    dos.writeShort(0xFEFF);

                    // add the current row to the byte stream
                    for (int i = 0; i < fieldValues.size(); i++) {
                        makeGetter(dos, fieldTypes.get(i), nullabilities.get(i), fieldValues.get(i));
                    }

                    totalRowsInserted += rowsInsertedIntoBaos;
                    // reset values for rows inserted and total number of bytes inserted
                    rowsInsertedIntoBaos = 1;
                    totalBytes = 2 + rowSize;
                }
            } catch (SQLException sqlException) {
                logger.error(sqlException.toString());
                throw sqlException;
            } catch (Exception e) {
                throw new SQLException("Error reading data provided");
            }
        }

        // add the remaining rows inside bytestream to buffers listbuffer of byte arrays
        if (totalRowsInserted < rows.size()) {
            miniBatchNum += 1;
            Triple newtriple = new Triple(miniBatchNum, rowsInsertedIntoBaos, baos.toByteArray());
            buffers.add(newtriple);
        }
        dos.close();
        return buffers;
    }

    private boolean checkNullable(DataOutputStream dos, boolean nullable, boolean isNull) throws SQLException, IOException {
        if (isNull) {
            // set null indicator (-1) if the column is allowed to be null
            if (nullable) {
                // when the value is null and the null indicator is set (-1), no other values from this column are expected
                // therefore, the rest of this current column is ignored and the next value expected is the null indicator of the next column
                dos.writeByte(-1);
                return true;
            } else {
                // return error for trying to submit null value when not permitted
                throw new SQLException("Column does not accept null values, received null value");
            }
        }
        return false;
    }
    
    // function definitions for each spark data type
    private void makeGetter(DataOutputStream dos, Schema.Type fieldType, boolean nullable, Object value)  throws SQLException, IOException {
        boolean isNull = false;
        switch (fieldType) {
            case BOOLEAN:
                isNull = checkNullable(dos, nullable, (value == null));
                if (!isNull) {
                    dos.writeByte(0x01);
                    
                    // the following values are accepted as boolean castable
                    switch (value.getClass().getName()) {
                        case "java.lang.String":
                            String valstr = value.toString().toUpperCase();
                            if( valstr == "YES" || valstr == "Y" || valstr == "ON" || valstr == "T" ){
                                dos.writeShort(0x01);
                            } else {
                                // if( valstr == "NO" || valstr == "N" || valstr == "OFF" || valstr == "F" ){
                                dos.writeShort(0x00);
                            }
                            break;
                        case "java.lang.Boolean":
                            if ((boolean)value) { 
                                dos.writeShort(0x01);
                            } else {
                                dos.writeShort(0x00);
                            }
                            break;
                        case "java.lang.Double":
                            if ((double)value > 0) { 
                                dos.writeShort(0x01);
                            } else {
                                dos.writeShort(0x00);
                            }
                            break;
                        case "java.lang.Float": 
                            if ((float)value > 0) {
                                dos.writeShort(0x01);
                            } else {
                                dos.writeShort(0x00);
                            }
                            break;
                        case "java.lang.Integer": 
                            if ((int)value > 0) {
                                dos.writeShort(0x01);
                            } else {
                                dos.writeShort(0x00);
                            }
                            break;
                        default:
                            throw new SQLException("Incorrect boolean format entered");
                    }
                }
                break;
            case INT8:
                isNull = checkNullable(dos, nullable, (value == null));
                if (!isNull) {
                    dos.writeByte(0x01);
                    dos.writeShort((byte)value);
                }
                break;
            case FLOAT64:
                isNull = checkNullable(dos, nullable, (value == null));
                if (!isNull) {
                    dos.writeByte(0x01);
                    dos.writeDouble((double)value);
                }
                break;
            case FLOAT32:
                isNull = checkNullable(dos, nullable, (value == null));
                if (!isNull) {
                    dos.writeByte(0x01);
                    dos.writeFloat((float)value);
                }
                break;
            case INT32:
                isNull = checkNullable(dos, nullable, (value == null));
                if (!isNull) {
                    dos.writeByte(0x01);
                    dos.writeInt((int)value);
                }
                break;
            case INT64:
                isNull = checkNullable(dos, nullable, (value == null));
                if (!isNull) {
                    dos.writeByte(0x01);
                    dos.writeLong((long)value);
                }
                break;
            case INT16:
                isNull = checkNullable(dos, nullable, (value == null));
                if (!isNull) {
                    dos.writeByte(0x01);
                    dos.writeShort((short)value);
                }
                break;
            case STRING:
                if (value == null) {
                    // null string values are represented by using negative values as the string length
                    if (nullable) {
                        dos.writeByte(-1);
                        dos.writeByte(-1);
                    } else {
                        // return error for trying to submit null value when not permitted
                        throw new SQLException("Column does not accept null values, received null value");
                    }
                } else {
                    String valstr = (String)value;
                    // string length is represented as two bytes per character, this is why we write string length*2
                    dos.writeShort(valstr.length()*2);
                    // chars to represent the string itself
                    dos.writeChars(valstr);
                }
                break;
            default:
                throw new SQLException("Unsupported type " + fieldType);
        }
    }

    private String formatMemoryTableSQLInsert(String tableName, Schema schema, List<String> fieldNames) throws SQLException {
        // This function is responsible for formatting the SQL query used for inserts (using MEMORY_TABLE insert subselects)

        // from the schema, find the db2 column names & types to be used in the memory table SQL command

        // the limit of the MEMORY_TABLE function
        int memoryTableInsertBufferLimitInGB = 2;
        // list of column names
        String colNames = "";
        String inputColNames = "";
        // list of column names + data types
        String db2Sql = "";

        List<Schema.Type> fieldTypes = new ArrayList<Schema.Type>();

        for (Field field : schema.fields()) {
            fieldTypes.add(field.schema().type());
        }

        for (int i = 0; i < fieldNames.size(); i++) {
            /**
             Find the db2map type for each column data type in the schema, add to the total db2sql statement.
                The schema column type is boolean, use smallint instead as the type, and add a cast back to boolean.
                If the schema column type is decimal or decfloat, use varchar, and cast back to original.
                */

            // variable representing list of columns (db2 data types)
            String newColSql = "";
            switch (fieldTypes.get(i)) {
                case BOOLEAN:
                    newColSql = fieldNames.get(i)  + " " + "SMALLINT";
                    colNames = colNames + "CAST(" + fieldNames.get(i)  + " AS BOOLEAN)";
                    inputColNames = inputColNames + fieldNames.get(i);
                    break;
                case INT8:
                case INT16:
                case FLOAT32:
                case INT32:
                case FLOAT64:
                case INT64:
                case STRING:
                    newColSql = fieldNames.get(i) + " " + commandBuilder.getDB2FieldType(fieldTypes.get(i));
                    colNames = colNames + fieldNames.get(i);
                    inputColNames = inputColNames + fieldNames.get(i);
                    break;
                default:
                    throw new SQLException("Unsupported type");
            }

            // add the column data type + name to the insert subselect command
            db2Sql += newColSql;
            // add commas if there are still more columns to be processed
            if (i != fieldNames.size() -1) {
                db2Sql = db2Sql + ", ";
                colNames = colNames + ", ";
                inputColNames = inputColNames + ", ";
            }
        }

        // create the SQL insert subselect query, insert BLOB and db2 SQL into Memory Table
        String sql = "INSERT INTO " + tableName + " ( " + inputColNames + " ) " +
                " SELECT " + colNames +
                " FROM TABLE( SYSPROC.MEMORY_TABLE( CAST(? AS BLOB(" + memoryTableInsertBufferLimitInGB + "G)) ) ) " +
                " AS T(" + db2Sql + ")";
        return sql;
    }

    // this api takes byte buffer <= memory memory table limit in length, uses this as input for memory table in an INSERT statement,
    // executed through JDBC
    private boolean batchInsertAsyncNewSendData(String sql, byte[] buffer, int numRows, int miniBatchNum) throws SQLException {
        logger.debug("Entered batchInsertAsyncNewSendData() function to insert data using Memory Table and JDBC");
        long startTime = System.currentTimeMillis();
        boolean returnValue = false;

        // connect to JDBC
        logger.debug("connecting to JDBC");
        

        try {
            SerialBlob blob = new SerialBlob(buffer);

            logger.debug("SQL code to be executed: " + sql);

            int rs = 0;
            // execute the SQL query through JDBC
            // connect to JDBC and prepare/execute sql statement
            PreparedStatement pstmt = dataSourceFactor.prepareStatement(sql);
            pstmt.setBlob(1, blob);

            logger.debug("execute the SQL statement");
            rs = pstmt.executeUpdate();
           
            if (rs != numRows) {
                returnValue = false;
                logger.debug("Incorrect Number of Rows Inputted. Expected: " + numRows + ", Inputted: " + rs);
            } else {
                returnValue = true;
                long endTime = System.currentTimeMillis();
                logger.debug("Rows inserted successfully via Memory table numrows = " + numRows + " in time = " + (endTime - startTime));
            }
            pstmt.close();
        } catch (SerialException se) {
            logger.error("Serial Exception issue: " + se.getMessage());
            throw new SQLException("Serial Exception issue: " + se.getMessage());
        } catch (SQLException sqlex) {
            logger.error("Client SQL request failed: " + sqlex.getMessage());
            throw sqlex;
        }
        
        logger.debug("Return value from Memory table INSERT: " + returnValue);
        return returnValue;
    }
}

class Triple/*<int, int, byte[]>*/ {

    private final int left;
    private final int middle;
    private final byte[] right;

    public Triple(int left, int middle, byte[] right) {
        this.left = left;
        this.middle = middle;
        this.right = right;
    }

    public int getLeft() {
        return left;
    }

    public int getMiddle() {
        return middle;
    }

    public byte[] getRight() {
        return right;
    }

    public String toString() {
        return left + " " + middle + " " + right;
    }
}

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.job.util;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.jdbc.client.JdbcClient;
import org.apache.doris.datasource.jdbc.util.JdbcFieldSchema;
import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.common.DataSourceType;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.GlobalVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class StreamingJobUtilsTest {

    @Mock
    private JdbcClient jdbcClient;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCdcDriverPrivilege() {
        Env env = Mockito.mock(Env.class);
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getAccessManager()).thenReturn(accessManager);
            List<String> remoteDrivers = List.of("http://example.invalid/driver.jar",
                    "https://example.invalid/driver.jar");
            for (String driver : remoteDrivers) {
                AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                        () -> StreamingJobUtils.checkCdcDriverPrivilege(ctx, driver));
                Assertions.assertTrue(error.getMessage().contains("ADMIN"));
            }
            Mockito.when(accessManager.checkGlobalPriv(ctx, PrivPredicate.ADMIN)).thenReturn(true);
            for (String driver : remoteDrivers) {
                Assertions.assertDoesNotThrow(() -> StreamingJobUtils.checkCdcDriverPrivilege(ctx, driver));
            }
            Mockito.when(accessManager.checkGlobalPriv(ctx, PrivPredicate.ADMIN)).thenReturn(false);
            for (String driver : Arrays.asList(null, "driver.jar", "file:///opt/doris/driver.jar")) {
                Assertions.assertDoesNotThrow(() -> StreamingJobUtils.checkCdcDriverPrivilege(ctx, driver));
            }
        }
    }

    @Test
    public void testBinarySourceColumnsUseSupportedOlapTypes() {
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("payload", ScalarType.createType(PrimitiveType.VARBINARY)));
        columns.add(new Column("id", ScalarType.createType(PrimitiveType.VARBINARY)));
        Mockito.when(jdbcClient.getColumnsFromJdbc("source_db", "source_table")).thenReturn(columns);

        List<Column> result = StreamingJobUtils.getColumns(
                jdbcClient, "source_db", "source_table", Arrays.asList("id"));

        Assertions.assertEquals("id", result.get(0).getName());
        Assertions.assertEquals(PrimitiveType.VARCHAR, result.get(0).getDataType());
        Assertions.assertEquals(ScalarType.MAX_VARCHAR_LENGTH, result.get(0).getType().getLength());
        Assertions.assertEquals(PrimitiveType.STRING, result.get(1).getDataType());
    }

    @Test
    public void testNestedBinaryColumnsMatchCdcJsonCarrier() {
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("payloads", new ArrayType(ScalarType.createVarbinaryType(16))));
        columns.add(new Column("nested", new ArrayType(new ArrayType(ScalarType.createVarbinaryType(16)))));
        Mockito.when(jdbcClient.getColumnsFromJdbc("source_db", "source_table")).thenReturn(columns);

        List<Column> result = StreamingJobUtils.getColumns(
                jdbcClient, "source_db", "source_table", new ArrayList<>());

        Assertions.assertEquals(new ArrayType(ScalarType.createStringType()), result.get(0).getType());
        Assertions.assertEquals(new ArrayType(new ArrayType(ScalarType.createStringType())), result.get(1).getType());
    }

    @Test
    public void testCdcRetainsTimestampCarrierIndependentlyOfJdbcCatalogMapping() {
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("event_time", ScalarType.createTimeStampTzType(6)));
        columns.add(new Column("millis", ScalarType.createTimeStampTzType(3)));
        columns.add(new Column("seconds", ScalarType.createTimeStampTzType(0)));
        columns.add(new Column("local_time", ScalarType.createDatetimeV2Type(6)));
        columns.add(new Column("events", new ArrayType(new ArrayType(ScalarType.createTimeStampTzType(6)))));
        Mockito.when(jdbcClient.getColumnsFromJdbc("source_db", "source_table")).thenReturn(columns);

        List<Column> result = StreamingJobUtils.getColumns(
                jdbcClient, "source_db", "source_table", Arrays.asList("event_time"));

        // CDC emits wall-clock carriers for both snapshots and binlog records, including keys.
        Assertions.assertEquals(ScalarType.createDatetimeV2Type(6), result.get(0).getType());
        Assertions.assertEquals(ScalarType.createDatetimeV2Type(3), result.get(1).getType());
        Assertions.assertEquals(ScalarType.createDatetimeV2Type(0), result.get(2).getType());
        Assertions.assertEquals(ScalarType.createDatetimeV2Type(6), result.get(3).getType());
        Assertions.assertEquals(new ArrayType(new ArrayType(ScalarType.createDatetimeV2Type(6))),
                result.get(4).getType());
    }

    @Test
    public void testGetColumnsWithPrimaryKeySorting() throws Exception {
        // Prepare test data
        String database = "test_db";
        String table = "test_table";
        List<String> primaryKeys = Arrays.asList("id", "name");

        // Create mock columns in random order
        List<Column> mockColumns = new ArrayList<>();
        mockColumns.add(new Column("age", ScalarType.createType(PrimitiveType.INT)));
        mockColumns.add(new Column("id", ScalarType.createType(PrimitiveType.BIGINT)));
        mockColumns.add(new Column("email", ScalarType.createVarcharType(100)));
        mockColumns.add(new Column("name", ScalarType.createVarcharType(50)));
        mockColumns.add(new Column("address", ScalarType.createVarcharType(200)));

        Mockito.when(jdbcClient.getColumnsFromJdbc(ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(mockColumns);
        List<Column> result = StreamingJobUtils.getColumns(jdbcClient, database, table, primaryKeys);

        // Verify primary keys are at the front in correct order
        Assertions.assertEquals(5, result.size());
        Assertions.assertEquals("id", result.get(0).getName());
        Assertions.assertEquals("name", result.get(1).getName());
        // Verify varchar primary key columns have their length multiplied by 3
        Column nameColumn = result.get(1);
        Assertions.assertEquals(150, nameColumn.getType().getLength()); // 50 * 3
        // Verify non-primary key columns follow
        Assertions.assertEquals("age", result.get(2).getName());
        Assertions.assertEquals("email", result.get(3).getName());
        Assertions.assertEquals("address", result.get(4).getName());
        // Verify non-primary key varchar columns also have their length multiplied by 3
        Column emailColumn = result.get(3);
        Assertions.assertEquals(300, emailColumn.getType().getLength()); // 100 * 3
        Column addressColumn = result.get(4);
        Assertions.assertEquals(600, addressColumn.getType().getLength()); // 200 * 3
    }

    @Test
    public void testGetColumnsWithVarcharTypeConversion() throws Exception {
        String database = "test_db";
        String table = "test_table";
        List<String> primaryKeys = Arrays.asList("id");

        List<Column> mockColumns = new ArrayList<>();
        mockColumns.add(new Column("id", ScalarType.createType(PrimitiveType.INT)));
        mockColumns.add(new Column("short_name", ScalarType.createVarcharType(50)));
        mockColumns.add(new Column("long_name", ScalarType.createVarcharType(20000)));

        Mockito.when(jdbcClient.getColumnsFromJdbc(ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(mockColumns);
        List<Column> result = StreamingJobUtils.getColumns(jdbcClient, database, table, primaryKeys);

        // Verify varchar length multiplication by 3
        Column shortName = result.stream()
                .filter(col -> col.getName().equals("short_name"))
                .findFirst()
                .orElse(null);
        Assertions.assertNotNull(shortName);
        Assertions.assertEquals(150, shortName.getType().getLength()); // 50 * 3

        // Verify long varchar becomes STRING type
        Column longName = result.stream()
                .filter(col -> col.getName().equals("long_name"))
                .findFirst()
                .orElse(null);
        Assertions.assertNotNull(longName);
        Assertions.assertTrue(longName.getType().isStringType());
    }

    @Test
    public void testGetColumnsWithStringTypeAsPrimaryKey() throws Exception {
        String database = "test_db";
        String table = "test_table";
        List<String> primaryKeys = Arrays.asList("id");

        List<Column> mockColumns = new ArrayList<>();
        mockColumns.add(new Column("id", ScalarType.createStringType()));
        mockColumns.add(new Column("name", ScalarType.createVarcharType(50)));

        Mockito.when(jdbcClient.getColumnsFromJdbc(ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(mockColumns);
        List<Column> result = StreamingJobUtils.getColumns(jdbcClient, database, table, primaryKeys);

        // Verify string type primary key is converted to varchar
        Column idColumn = result.stream()
                .filter(col -> col.getName().equals("id"))
                .findFirst()
                .orElse(null);
        Assertions.assertNotNull(idColumn);
        Assertions.assertTrue(idColumn.getType().isVarchar());
        Assertions.assertEquals(ScalarType.MAX_VARCHAR_LENGTH, idColumn.getType().getLength());
    }

    @Test
    public void testGetColumnsWithEmptyPrimaryKeys() throws Exception {
        String database = "test_db";
        String table = "test_table";
        List<String> primaryKeys = new ArrayList<>();

        List<Column> mockColumns = new ArrayList<>();
        mockColumns.add(new Column("col1", ScalarType.createType(PrimitiveType.INT)));
        mockColumns.add(new Column("col2", ScalarType.createVarcharType(100)));
        mockColumns.add(new Column("col3", ScalarType.createType(PrimitiveType.BIGINT)));

        Mockito.when(jdbcClient.getColumnsFromJdbc(ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(mockColumns);
        List<Column> result = StreamingJobUtils.getColumns(jdbcClient, database, table, primaryKeys);

        // Verify columns maintain original order when no primary keys
        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals("col1", result.get(0).getName());
        Assertions.assertEquals("col2", result.get(1).getName());
        Assertions.assertEquals("col3", result.get(2).getName());
    }

    @Test
    public void testGetColumnsWithMultiplePrimaryKeys() throws Exception {
        String database = "test_db";
        String table = "test_table";
        List<String> primaryKeys = Arrays.asList("pk3", "pk1", "pk2");

        List<Column> mockColumns = new ArrayList<>();
        mockColumns.add(new Column("data1", ScalarType.createType(PrimitiveType.INT)));
        mockColumns.add(new Column("pk1", ScalarType.createType(PrimitiveType.INT)));
        mockColumns.add(new Column("data2", ScalarType.createVarcharType(100)));
        mockColumns.add(new Column("pk2", ScalarType.createType(PrimitiveType.BIGINT)));
        mockColumns.add(new Column("pk3", ScalarType.createType(PrimitiveType.INT)));
        mockColumns.add(new Column("data3", ScalarType.createVarcharType(50)));

        Mockito.when(jdbcClient.getColumnsFromJdbc(ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(mockColumns);
        List<Column> result = StreamingJobUtils.getColumns(jdbcClient, database, table, primaryKeys);

        // Verify primary keys are sorted in the order defined in primaryKeys list
        Assertions.assertEquals(6, result.size());
        Assertions.assertEquals("pk3", result.get(0).getName());
        Assertions.assertEquals("pk1", result.get(1).getName());
        Assertions.assertEquals("pk2", result.get(2).getName());
        // Verify non-primary keys follow
        Assertions.assertEquals("data1", result.get(3).getName());
        Assertions.assertEquals("data2", result.get(4).getName());
        Assertions.assertEquals("data3", result.get(5).getName());
    }

    @Test
    public void testGetColumnsWithUnsupportedColumnType() throws Exception {
        String database = "test_db";
        String table = "test_table";
        List<String> primaryKeys = Arrays.asList("id");

        List<Column> mockColumns = new ArrayList<>();
        mockColumns.add(new Column("id", ScalarType.createType(PrimitiveType.INT)));
        mockColumns.add(new Column("unsupported_col", new ScalarType(PrimitiveType.UNSUPPORTED)));

        Mockito.when(jdbcClient.getColumnsFromJdbc(ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(mockColumns);
        // This should throw IllegalArgumentException due to unsupported column type
        try {
            StreamingJobUtils.getColumns(jdbcClient, database, table, primaryKeys);
            Assertions.fail("Expected IllegalArgumentException to be thrown");
        } catch (IllegalArgumentException e) {
            // Verify the exception message contains expected information
            String message = e.getMessage();
            Assertions.assertTrue(message.contains("Unsupported column type"));
            Assertions.assertTrue(message.contains("test_table"));
            Assertions.assertTrue(message.contains("unsupported_col"));
        }
    }

    @Test
    public void testGetColumnsWithVarcharPrimaryKeyLengthMultiplication() throws Exception {
        String database = "test_db";
        String table = "test_table";
        List<String> primaryKeys = Arrays.asList("pk_varchar", "pk_int");

        List<Column> mockColumns = new ArrayList<>();
        mockColumns.add(new Column("pk_int", ScalarType.createType(PrimitiveType.INT)));
        mockColumns.add(new Column("pk_varchar", ScalarType.createVarcharType(100)));
        mockColumns.add(new Column("normal_varchar", ScalarType.createVarcharType(50)));

        Mockito.when(jdbcClient.getColumnsFromJdbc(ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(mockColumns);
        List<Column> result = StreamingJobUtils.getColumns(jdbcClient, database, table, primaryKeys);

        // Verify varchar primary key column has length multiplied by 3
        Column pkVarcharColumn = result.stream()
                .filter(col -> col.getName().equals("pk_varchar"))
                .findFirst()
                .orElse(null);
        Assertions.assertNotNull(pkVarcharColumn);
        Assertions.assertEquals(300, pkVarcharColumn.getType().getLength()); // 100 * 3

        // Verify normal varchar column also has length multiplied by 3
        Column normalVarcharColumn = result.stream()
                .filter(col -> col.getName().equals("normal_varchar"))
                .findFirst()
                .orElse(null);
        Assertions.assertNotNull(normalVarcharColumn);
        Assertions.assertEquals(150, normalVarcharColumn.getType().getLength()); // 50 * 3
    }

    @Test
    public void testGetOceanBaseRemoteDbName() {
        Map<String, String> properties = new HashMap<>();
        properties.put(DataSourceConfigKeys.DATABASE, "test_db");

        Assertions.assertEquals("test_db",
                StreamingJobUtils.getRemoteDbName(DataSourceType.OCEANBASE, properties));
    }

    @Test
    public void testGenerateCreateTableCmdsClosesJdbcClientOnFailure() {
        Map<String, String> properties = new HashMap<>();
        try (MockedStatic<StreamingJobUtils> utils = Mockito.mockStatic(StreamingJobUtils.class,
                Mockito.CALLS_REAL_METHODS)) {
            utils.when(() -> StreamingJobUtils.getJdbcClient(DataSourceType.OCEANBASE, properties))
                    .thenReturn(jdbcClient);
            utils.when(() -> StreamingJobUtils.getRemoteDbName(DataSourceType.OCEANBASE, properties))
                    .thenReturn("test_db");
            Mockito.when(jdbcClient.getTablesNameList("test_db")).thenReturn(new ArrayList<>());

            Assertions.assertThrows(JobException.class, () -> StreamingJobUtils.generateCreateTableCmds(
                    "target_db", DataSourceType.OCEANBASE, properties, new HashMap<>()));

            Mockito.verify(jdbcClient).closeClient();
        }
    }

    @Test
    public void testGenerateCreateTableCmdsFindsMixedCasePrecreatedTargetWhenStoredLowerCase() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(DataSourceConfigKeys.SCHEMA, "source_db");
        properties.put(DataSourceConfigKeys.TABLE + ".source_table."
                + DataSourceConfigKeys.TABLE_TARGET_TABLE_SUFFIX, "MixedTarget");

        Database targetDatabase = new Database(1L, "target_db");
        targetDatabase.registerTable(new OlapTable(2L, "mixedtarget", new ArrayList<>(), KeysType.UNIQUE_KEYS,
                null, null));
        Env env = Mockito.mock(Env.class);
        InternalCatalog internalCatalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(env.getInternalCatalog()).thenReturn(internalCatalog);
        Mockito.when(internalCatalog.getDbNullable("target_db")).thenReturn(targetDatabase);
        Mockito.when(jdbcClient.getTablesNameList("source_db"))
                .thenReturn(Arrays.asList("source_table"));
        Mockito.when(jdbcClient.getPrimaryKeys("source_db", "source_table"))
                .thenReturn(Arrays.asList("id"));
        Mockito.when(jdbcClient.getColumnsFromJdbc("source_db", "source_table"))
                .thenReturn(Arrays.asList(
                        new Column("id", ScalarType.createType(PrimitiveType.INT)),
                        new Column("unsupported_col", new ScalarType(PrimitiveType.UNSUPPORTED))));

        int originalLowerCaseTableNames = GlobalVariable.lowerCaseTableNames;
        GlobalVariable.lowerCaseTableNames = 1;
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
                MockedStatic<StreamingJobUtils> utils = Mockito.mockStatic(StreamingJobUtils.class,
                        Mockito.CALLS_REAL_METHODS)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            utils.when(() -> StreamingJobUtils.getJdbcClient(DataSourceType.POSTGRES, properties))
                    .thenReturn(jdbcClient);

            Assertions.assertFalse(StreamingJobUtils.generateCreateTableCmds(
                    "target_db", DataSourceType.POSTGRES, properties, new HashMap<>())
                    .get("source_table").isPresent());
        } finally {
            GlobalVariable.lowerCaseTableNames = originalLowerCaseTableNames;
        }
    }

    private List<Column> buildPostGisBuildingColumns() {
        return Arrays.asList(
                new Column("id", ScalarType.createType(PrimitiveType.BIGINT)),
                new Column("buildingid", ScalarType.createType(PrimitiveType.BIGINT)),
                new Column("buildingdata", ScalarType.createStringType()),
                new Column("buildingclass", ScalarType.createType(PrimitiveType.BIGINT)),
                new Column("buildingname", ScalarType.createVarcharType(255)),
                new Column("geometry", new ScalarType(PrimitiveType.UNSUPPORTED)),
                new Column("create_by", ScalarType.createVarcharType(32)),
                new Column("create_time", ScalarType.createDatetimeV2Type(6)),
                new Column("update_by", ScalarType.createVarcharType(32)),
                new Column("update_time", ScalarType.createDatetimeV2Type(6)),
                new Column("processdata", ScalarType.createStringType()),
                new Column("lng", ScalarType.createDecimalV3Type(16, 8)),
                new Column("lat", ScalarType.createDecimalV3Type(16, 8)));
    }

    @Test
    public void testGenerateCreateTableCmdsExcludesUnsupportedPostGisGeometryColumn() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(DataSourceConfigKeys.SCHEMA, "public");
        properties.put(DataSourceConfigKeys.INCLUDE_TABLES, "building");
        properties.put(DataSourceConfigKeys.TABLE + ".building."
                + DataSourceConfigKeys.TABLE_EXCLUDE_COLUMNS_SUFFIX, "geometry");

        Database targetDatabase = new Database(1L, "szt_ods");
        Env env = Mockito.mock(Env.class);
        InternalCatalog internalCatalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(env.getInternalCatalog()).thenReturn(internalCatalog);
        Mockito.when(internalCatalog.getDbNullable("szt_ods")).thenReturn(targetDatabase);
        Mockito.when(jdbcClient.getTablesNameList("public")).thenReturn(Arrays.asList("building"));
        Mockito.when(jdbcClient.getPrimaryKeys("public", "building")).thenReturn(Arrays.asList("id"));
        Mockito.when(jdbcClient.getColumnsFromJdbc("public", "building"))
                .thenReturn(buildPostGisBuildingColumns());

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
                MockedStatic<StreamingJobUtils> utils = Mockito.mockStatic(StreamingJobUtils.class,
                        Mockito.CALLS_REAL_METHODS)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            utils.when(() -> StreamingJobUtils.getJdbcClient(DataSourceType.POSTGRES, properties))
                    .thenReturn(jdbcClient);

            LinkedHashMap<String, Optional<CreateTableCommand>> cmds = StreamingJobUtils.generateCreateTableCmds(
                    "szt_ods", DataSourceType.POSTGRES, properties, new HashMap<>());
            Assertions.assertTrue(cmds.containsKey("building"));
            Assertions.assertTrue(cmds.get("building").isPresent());
            List<ColumnDefinition> defs = cmds.get("building").get().getCreateTableInfo().getColumnDefinitions();
            List<String> colNames = defs.stream().map(ColumnDefinition::getName).collect(Collectors.toList());
            Assertions.assertEquals(12, colNames.size());
            Assertions.assertFalse(colNames.contains("geometry"));
            Assertions.assertEquals("id", colNames.get(0));
            ColumnDefinition buildingName = defs.stream()
                    .filter(c -> "buildingname".equals(c.getName()))
                    .findFirst()
                    .orElseThrow(AssertionError::new);
            Assertions.assertEquals(765, buildingName.getType().toCatalogDataType().getLength());
            Mockito.verify(jdbcClient).closeClient();
        }
    }

    @Test
    public void testGenerateCreateTableCmdsStillRejectsNonExcludedUnsupportedColumn() {
        Database targetDatabase = new Database(1L, "szt_ods");
        Env env = Mockito.mock(Env.class);
        InternalCatalog internalCatalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(env.getInternalCatalog()).thenReturn(internalCatalog);
        Mockito.when(internalCatalog.getDbNullable("szt_ods")).thenReturn(targetDatabase);
        Mockito.when(jdbcClient.getTablesNameList("public")).thenReturn(Arrays.asList("building"));
        Mockito.when(jdbcClient.getPrimaryKeys("public", "building")).thenReturn(Arrays.asList("id"));

        Map<String, String> withoutExclude = new HashMap<>();
        withoutExclude.put(DataSourceConfigKeys.SCHEMA, "public");
        withoutExclude.put(DataSourceConfigKeys.INCLUDE_TABLES, "building");

        Map<String, String> excludeOther = new HashMap<>(withoutExclude);
        excludeOther.put(DataSourceConfigKeys.TABLE + ".building."
                + DataSourceConfigKeys.TABLE_EXCLUDE_COLUMNS_SUFFIX, "buildingname");

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
                MockedStatic<StreamingJobUtils> utils = Mockito.mockStatic(StreamingJobUtils.class,
                        Mockito.CALLS_REAL_METHODS)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            utils.when(() -> StreamingJobUtils.getJdbcClient(DataSourceType.POSTGRES, withoutExclude))
                    .thenReturn(jdbcClient);
            utils.when(() -> StreamingJobUtils.getJdbcClient(DataSourceType.POSTGRES, excludeOther))
                    .thenReturn(jdbcClient);

            Mockito.when(jdbcClient.getColumnsFromJdbc("public", "building"))
                    .thenReturn(buildPostGisBuildingColumns());
            IllegalArgumentException errNoExclude = Assertions.assertThrows(IllegalArgumentException.class,
                    () -> StreamingJobUtils.generateCreateTableCmds(
                            "szt_ods", DataSourceType.POSTGRES, withoutExclude, new HashMap<>()));
            Assertions.assertTrue(errNoExclude.getMessage().contains("Unsupported column type"));
            Assertions.assertTrue(errNoExclude.getMessage().contains("geometry"));

            Mockito.when(jdbcClient.getColumnsFromJdbc("public", "building"))
                    .thenReturn(buildPostGisBuildingColumns());
            IllegalArgumentException errExcludeOther = Assertions.assertThrows(IllegalArgumentException.class,
                    () -> StreamingJobUtils.generateCreateTableCmds(
                            "szt_ods", DataSourceType.POSTGRES, excludeOther, new HashMap<>()));
            Assertions.assertTrue(errExcludeOther.getMessage().contains("Unsupported column type"));
            Assertions.assertTrue(errExcludeOther.getMessage().contains("geometry"));
        }
    }

    @Test
    public void testGenerateCreateTableCmdsHandlesMultipleUnsupportedColumnsAndCustomTarget() throws Exception {
        Database targetDatabase = new Database(1L, "szt_ods");
        Env env = Mockito.mock(Env.class);
        InternalCatalog internalCatalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(env.getInternalCatalog()).thenReturn(internalCatalog);
        Mockito.when(internalCatalog.getDbNullable("szt_ods")).thenReturn(targetDatabase);
        Mockito.when(jdbcClient.getTablesNameList("public")).thenReturn(Arrays.asList("building_multi"));
        Mockito.when(jdbcClient.getPrimaryKeys("public", "building_multi"))
                .thenReturn(Arrays.asList("pk2", "pk1"));

        Map<String, String> excludeOnlyFirst = new HashMap<>();
        excludeOnlyFirst.put(DataSourceConfigKeys.SCHEMA, "public");
        excludeOnlyFirst.put(DataSourceConfigKeys.INCLUDE_TABLES, "building_multi");
        excludeOnlyFirst.put(DataSourceConfigKeys.TABLE + ".building_multi."
                + DataSourceConfigKeys.TABLE_EXCLUDE_COLUMNS_SUFFIX, "leading_geom");

        Map<String, String> excludeBoth = new HashMap<>();
        excludeBoth.put(DataSourceConfigKeys.SCHEMA, "public");
        excludeBoth.put(DataSourceConfigKeys.INCLUDE_TABLES, "building_multi");
        excludeBoth.put(DataSourceConfigKeys.TABLE + ".building_multi."
                + DataSourceConfigKeys.TABLE_TARGET_TABLE_SUFFIX, "building_ods");
        excludeBoth.put(DataSourceConfigKeys.TABLE + ".building_multi."
                + DataSourceConfigKeys.TABLE_EXCLUDE_COLUMNS_SUFFIX, " leading_geom , trailing_geom ");

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
                MockedStatic<StreamingJobUtils> utils = Mockito.mockStatic(StreamingJobUtils.class,
                        Mockito.CALLS_REAL_METHODS)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            utils.when(() -> StreamingJobUtils.getJdbcClient(DataSourceType.POSTGRES, excludeOnlyFirst))
                    .thenReturn(jdbcClient);
            utils.when(() -> StreamingJobUtils.getJdbcClient(DataSourceType.POSTGRES, excludeBoth))
                    .thenReturn(jdbcClient);

            Mockito.when(jdbcClient.getColumnsFromJdbc("public", "building_multi"))
                    .thenReturn(Arrays.asList(
                            new Column("leading_geom", new ScalarType(PrimitiveType.UNSUPPORTED)),
                            new Column("pk1", ScalarType.createType(PrimitiveType.BIGINT)),
                            new Column("payload", ScalarType.createType(PrimitiveType.VARBINARY)),
                            new Column("pk2", ScalarType.createStringType()),
                            new Column("event_ts", ScalarType.createTimeStampTzType(6)),
                            new Column("trailing_geom", new ScalarType(PrimitiveType.UNSUPPORTED))));
            IllegalArgumentException partialErr = Assertions.assertThrows(IllegalArgumentException.class,
                    () -> StreamingJobUtils.generateCreateTableCmds(
                            "szt_ods", DataSourceType.POSTGRES, excludeOnlyFirst, new HashMap<>()));
            Assertions.assertTrue(partialErr.getMessage().contains("trailing_geom"),
                    () -> "Expected error on trailing_geom, got: " + partialErr.getMessage());
            Assertions.assertFalse(partialErr.getMessage().contains("leading_geom"),
                    () -> "Excluded leading_geom should not fail, got: " + partialErr.getMessage());

            Mockito.when(jdbcClient.getColumnsFromJdbc("public", "building_multi"))
                    .thenReturn(Arrays.asList(
                            new Column("leading_geom", new ScalarType(PrimitiveType.UNSUPPORTED)),
                            new Column("pk1", ScalarType.createType(PrimitiveType.BIGINT)),
                            new Column("payload", ScalarType.createType(PrimitiveType.VARBINARY)),
                            new Column("pk2", ScalarType.createStringType()),
                            new Column("event_ts", ScalarType.createTimeStampTzType(6)),
                            new Column("trailing_geom", new ScalarType(PrimitiveType.UNSUPPORTED))));
            LinkedHashMap<String, Optional<CreateTableCommand>> cmds = StreamingJobUtils.generateCreateTableCmds(
                    "szt_ods", DataSourceType.POSTGRES, excludeBoth, new HashMap<>());
            CreateTableCommand cmd = cmds.get("building_multi").orElseThrow(AssertionError::new);
            Assertions.assertEquals("building_ods", cmd.getCreateTableInfo().getTableName());
            List<ColumnDefinition> defs = cmd.getCreateTableInfo().getColumnDefinitions();
            Assertions.assertEquals(Arrays.asList("pk2", "pk1", "payload", "event_ts"),
                    defs.stream().map(ColumnDefinition::getName).collect(Collectors.toList()));
            Assertions.assertEquals(PrimitiveType.VARCHAR,
                    defs.get(0).getType().toCatalogDataType().getPrimitiveType());
            Assertions.assertEquals(PrimitiveType.STRING,
                    defs.get(2).getType().toCatalogDataType().getPrimitiveType());
            Assertions.assertEquals(ScalarType.createDatetimeV2Type(6), defs.get(3).getType().toCatalogDataType());
        }
    }

    @Test
    public void testGenerateCreateTableCmdsPreservesExcludeValidationWithUnsupportedColumn() {
        Database targetDatabase = new Database(1L, "szt_ods");
        Env env = Mockito.mock(Env.class);
        InternalCatalog internalCatalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(env.getInternalCatalog()).thenReturn(internalCatalog);
        Mockito.when(internalCatalog.getDbNullable("szt_ods")).thenReturn(targetDatabase);
        Mockito.when(jdbcClient.getTablesNameList("public")).thenReturn(Arrays.asList("building"));
        Mockito.when(jdbcClient.getPrimaryKeys("public", "building")).thenReturn(Arrays.asList("id"));

        Map<String, String> nonexistentProps = new HashMap<>();
        nonexistentProps.put(DataSourceConfigKeys.SCHEMA, "public");
        nonexistentProps.put(DataSourceConfigKeys.INCLUDE_TABLES, "building");
        nonexistentProps.put(DataSourceConfigKeys.TABLE + ".building."
                + DataSourceConfigKeys.TABLE_EXCLUDE_COLUMNS_SUFFIX, "geometry,nonexistent_col");

        Map<String, String> pkExcludeProps = new HashMap<>();
        pkExcludeProps.put(DataSourceConfigKeys.SCHEMA, "public");
        pkExcludeProps.put(DataSourceConfigKeys.INCLUDE_TABLES, "building");
        pkExcludeProps.put(DataSourceConfigKeys.TABLE + ".building."
                + DataSourceConfigKeys.TABLE_EXCLUDE_COLUMNS_SUFFIX, "id,geometry");

        Map<String, String> unsupportedPkProps = new HashMap<>();
        unsupportedPkProps.put(DataSourceConfigKeys.SCHEMA, "public");
        unsupportedPkProps.put(DataSourceConfigKeys.INCLUDE_TABLES, "building");
        unsupportedPkProps.put(DataSourceConfigKeys.TABLE + ".building."
                + DataSourceConfigKeys.TABLE_EXCLUDE_COLUMNS_SUFFIX, "geometry");

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
                MockedStatic<StreamingJobUtils> utils = Mockito.mockStatic(StreamingJobUtils.class,
                        Mockito.CALLS_REAL_METHODS)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            utils.when(() -> StreamingJobUtils.getJdbcClient(DataSourceType.POSTGRES, nonexistentProps))
                    .thenReturn(jdbcClient);
            utils.when(() -> StreamingJobUtils.getJdbcClient(DataSourceType.POSTGRES, pkExcludeProps))
                    .thenReturn(jdbcClient);
            utils.when(() -> StreamingJobUtils.getJdbcClient(DataSourceType.POSTGRES, unsupportedPkProps))
                    .thenReturn(jdbcClient);

            Mockito.when(jdbcClient.getColumnsFromJdbc("public", "building"))
                    .thenReturn(buildPostGisBuildingColumns());
            JobException nonexistentErr = Assertions.assertThrows(JobException.class,
                    () -> StreamingJobUtils.generateCreateTableCmds(
                            "szt_ods", DataSourceType.POSTGRES, nonexistentProps, new HashMap<>()));
            Assertions.assertTrue(nonexistentErr.getMessage().contains("does not exist"));
            Assertions.assertTrue(nonexistentErr.getMessage().contains("nonexistent_col"));

            Mockito.when(jdbcClient.getColumnsFromJdbc("public", "building"))
                    .thenReturn(buildPostGisBuildingColumns());
            JobException pkErr = Assertions.assertThrows(JobException.class,
                    () -> StreamingJobUtils.generateCreateTableCmds(
                            "szt_ods", DataSourceType.POSTGRES, pkExcludeProps, new HashMap<>()));
            Assertions.assertTrue(pkErr.getMessage().contains("primary key"));

            Mockito.when(jdbcClient.getPrimaryKeys("public", "building")).thenReturn(Arrays.asList("geometry"));
            Mockito.when(jdbcClient.getColumnsFromJdbc("public", "building"))
                    .thenReturn(buildPostGisBuildingColumns());
            JobException unsupportedPkErr = Assertions.assertThrows(JobException.class,
                    () -> StreamingJobUtils.generateCreateTableCmds(
                            "szt_ods", DataSourceType.POSTGRES, unsupportedPkProps, new HashMap<>()));
            Assertions.assertTrue(unsupportedPkErr.getMessage().contains("primary key"));
        }
    }

    @Test
    public void testGenerateCreateTableCmdsPrecreatedTargetStillValidatesExcludeColumns() throws Exception {
        Database targetDatabase = new Database(1L, "szt_ods");
        targetDatabase.registerTable(new OlapTable(2L, "building", new ArrayList<>(), KeysType.UNIQUE_KEYS,
                null, null));
        Env env = Mockito.mock(Env.class);
        InternalCatalog internalCatalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(env.getInternalCatalog()).thenReturn(internalCatalog);
        Mockito.when(internalCatalog.getDbNullable("szt_ods")).thenReturn(targetDatabase);
        Mockito.when(jdbcClient.getTablesNameList("public")).thenReturn(Arrays.asList("building"));
        Mockito.when(jdbcClient.getPrimaryKeys("public", "building")).thenReturn(Arrays.asList("id"));

        JdbcFieldSchema idField = Mockito.mock(JdbcFieldSchema.class);
        Mockito.when(idField.getColumnName()).thenReturn("id");
        JdbcFieldSchema geomField = Mockito.mock(JdbcFieldSchema.class);
        Mockito.when(geomField.getColumnName()).thenReturn("geometry");
        Mockito.when(jdbcClient.getJdbcColumnsInfo("public", "building"))
                .thenReturn(Arrays.asList(idField, geomField));

        Map<String, String> validProps = new HashMap<>();
        validProps.put(DataSourceConfigKeys.SCHEMA, "public");
        validProps.put(DataSourceConfigKeys.INCLUDE_TABLES, "building");
        validProps.put(DataSourceConfigKeys.TABLE + ".building."
                + DataSourceConfigKeys.TABLE_EXCLUDE_COLUMNS_SUFFIX, "geometry");

        Map<String, String> invalidProps = new HashMap<>();
        invalidProps.put(DataSourceConfigKeys.SCHEMA, "public");
        invalidProps.put(DataSourceConfigKeys.INCLUDE_TABLES, "building");
        invalidProps.put(DataSourceConfigKeys.TABLE + ".building."
                + DataSourceConfigKeys.TABLE_EXCLUDE_COLUMNS_SUFFIX, "missing_col");

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
                MockedStatic<StreamingJobUtils> utils = Mockito.mockStatic(StreamingJobUtils.class,
                        Mockito.CALLS_REAL_METHODS)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            utils.when(() -> StreamingJobUtils.getJdbcClient(DataSourceType.POSTGRES, validProps))
                    .thenReturn(jdbcClient);
            utils.when(() -> StreamingJobUtils.getJdbcClient(DataSourceType.POSTGRES, invalidProps))
                    .thenReturn(jdbcClient);

            LinkedHashMap<String, Optional<CreateTableCommand>> cmds = StreamingJobUtils.generateCreateTableCmds(
                    "szt_ods", DataSourceType.POSTGRES, validProps, new HashMap<>());
            Assertions.assertFalse(cmds.get("building").isPresent());

            JobException err = Assertions.assertThrows(JobException.class,
                    () -> StreamingJobUtils.generateCreateTableCmds(
                            "szt_ods", DataSourceType.POSTGRES, invalidProps, new HashMap<>()));
            Assertions.assertTrue(err.getMessage().contains("does not exist"));
        }
    }
}

package main;

import java.awt.Desktop;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import model.InputData;
import model.OutputData;

public class Main {

    // A => 表格定義
    // B => Index
    private static final String RUN_TYPE = "A";
    private static final String SCHEMA = "SCHEMA";
    private static final String TABLE = "TABLE";
    // ROWMAPPER 預設為以下格式，若非以下格式在自行填寫 ROWMAPPER 即可
    // 1. TB_APPR_DOC => ApprDocMapper
    // 2. TT_MQ_QUERY => TtMqQueryMapper
    private static String ROWMAPPER = "";

    // 填 DB USER
    private static final String DB_USER = "DB_USER";
    // Mapper 搜尋路徑
    private static final String PROJECT_PATH = "PROJECT_PATH";
    // SVN (CBP0304存放款專案共用) 路徑
    private static final String SVN_PATH = "SVN_PATH";
    private static Map<String, String> columnPoMapping;

    public static void main(String[] args) throws Exception {
        if ("A".equals(RUN_TYPE)) {
            runA();
            openWordFile();
        } else if ("B".equals(RUN_TYPE)) {
            runB();
        }
    }

    private static void runA() throws ClassNotFoundException {
        initA();
        Class.forName("oracle.jdbc.OracleDriver");
        try (Connection connection = DriverManager.getConnection(getDbUrl(), DB_USER, DB_USER)) {
            Statement statement = connection.createStatement();
            ResultSet result = statement.executeQuery(getQuerySql());
            List<OutputData> outputDataList = new ArrayList<>();
            while (result.next()) {
                InputData inputData = new InputData();
                inputData.setColumnId(result.getString("COLUMN_ID"));
                inputData.setConstraintType(result.getString("CONSTRAINT_TYPE"));
                inputData.setColumnName(result.getString("COLUMN_NAME"));
                inputData.setComments(result.getString("COMMENTS"));
                inputData.setDataType(result.getString("DATA_TYPE"));
                inputData.setCharLength(result.getInt("CHAR_LENGTH"));
                inputData.setDataLength(result.getInt("DATA_LENGTH"));
                inputData.setDataPrecision(result.getInt("DATA_PRECISION"));
                inputData.setDataScale(result.getInt("DATA_SCALE"));
                inputData.setNullable(result.getString("NULLABLE"));
                inputData.setDataDefault(result.getString("DATA_DEFAULT"));
                outputDataList.add(inputDataToOutputData(inputData));
            }
            writeOutputData(outputDataList);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void initA() {
        if ("".equals(ROWMAPPER.trim())) {
            ROWMAPPER = getRowMapper();
        }
        try {
            List<Path> rowMapperPaths = Files.walk(Paths.get(PROJECT_PATH))
                    .filter(Files::isRegularFile)
                    .filter(file -> {
                        return file.getFileName().toString().equals(ROWMAPPER + ".java");
                    }).collect(Collectors.toList());

            if (rowMapperPaths.size() == 0) {
                System.err.println(String.format("找不到預設 RowMapper! 請自行設定 RowMapper 或 調整路徑!"));
            } else if (rowMapperPaths.size() > 1) {
                System.err.println(String.format("你有 %d 個 RowMapper，下方使用第一個執行!", rowMapperPaths.size()));
                rowMapperPaths.forEach(rowMapperPath -> {
                    System.out.println(rowMapperPath.toString());
                });
            }

            columnPoMapping = new HashMap<>();
            List<String> rowMapperFile = Files.readAllLines(rowMapperPaths.get(0));
            rowMapperFile.stream()
                    .filter(line -> line.contains("\""))
                    .forEach(line -> {
                        String key = line.split("\"")[1];
                        String value = line.split(".set")[1].split("\\(")[0];
                        value = Character.toLowerCase(value.charAt(0)) + value.substring(1);
                        columnPoMapping.put(key, value);
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String getRowMapper() {
        StringBuilder rowMapper = new StringBuilder();
        String[] tableBlock = TABLE.split("_");
        if ("TB".equals(tableBlock[0])) {
            for (int i = 1; i < tableBlock.length; i++) {
                rowMapper.append(tableBlock[i].charAt(0));
                rowMapper.append(tableBlock[i].substring(1).toLowerCase());
            }
        } else if ("TT".equals(tableBlock[0])) {
            for (String tt : tableBlock) {
                rowMapper.append(tt.charAt(0));
                rowMapper.append(tt.substring(1).toLowerCase());
            }
        }
        rowMapper.append("Mapper");
        return rowMapper.toString();
    }

    private static String getDbUrl() {
        return "jdbc:oracle:thin:@ip:port/schema";
    }

    private static String getQuerySql() {
        return String.format("WITH CONSTRAINTS AS (\r\n" +
                "    SELECT constraints.CONSTRAINT_TYPE,\r\n" +
                "           columns.COLUMN_NAME\r\n" +
                "      FROM ALL_CONSTRAINTS constraints\r\n" +
                "      JOIN ALL_CONS_COLUMNS columns\r\n" +
                "        ON constraints.CONSTRAINT_NAME = columns.CONSTRAINT_NAME\r\n" +
                "     WHERE constraints.OWNER = '%s'\r\n" +
                "       AND constraints.TABLE_NAME = '%s'\r\n" +
                "       AND constraints.CONSTRAINT_TYPE IN ('P', 'U')" +
                ")\r\n" +
                "SELECT detail.COLUMN_ID,\r\n" +
                "       CONSTRAINTS.CONSTRAINT_TYPE,\r\n" +
                "       detail.COLUMN_NAME,\r\n" +
                "       comments.COMMENTS,\r\n" +
                "       detail.DATA_TYPE,\r\n" +
                "       detail.CHAR_LENGTH,\r\n" +
                "       detail.DATA_LENGTH,\r\n" +
                "       detail.DATA_PRECISION,\r\n" +
                "       detail.DATA_SCALE,\r\n" +
                "       detail.NULLABLE,\r\n" +
                "       detail.DATA_DEFAULT\r\n" +
                "  FROM ALL_TAB_COLUMNS detail\r\n" +
                "  LEFT JOIN CONSTRAINTS\r\n" +
                "    ON detail.COLUMN_NAME = CONSTRAINTS.COLUMN_NAME\r\n" +
                "  LEFT JOIN ALL_COL_COMMENTS comments\r\n" +
                "    ON comments.OWNER = '%s'\r\n" +
                "   AND comments.TABLE_NAME = '%s'\r\n" +
                "   AND detail.COLUMN_NAME = comments.COLUMN_NAME\r\n" +
                " WHERE detail.OWNER = '%s'\r\n" +
                "   AND detail.TABLE_NAME = '%s'\r\n" +
                " ORDER BY COLUMN_ID", SCHEMA, TABLE, SCHEMA, TABLE, SCHEMA, TABLE);
    }

    private static OutputData inputDataToOutputData(InputData inputData) throws IOException {
        OutputData outputData = new OutputData();
        outputData.setId(inputData.getColumnId());
        outputData.setKey(constraintTypeToKey(inputData.getConstraintType()));
        outputData.setColumnName(inputData.getColumnName());
        outputData.setComments(inputData.getComments());
        outputData.setType(inputData.getDataType());
        outputData.setLength(processLength(inputData));
        outputData.setNullable(inputData.getNullable());
        outputData.setDefaultValue(inputData.getDataDefault());
        outputData.setMappingPoColumnName(columnPoMapping.get(inputData.getColumnName()));
        return outputData;
    }

    private static String constraintTypeToKey(String constraintType) {
        String key = null;
        if ("P".equals(constraintType)) {
            key = "PK";
        } else if ("U".equals(constraintType)) {
            key = "UK";
        }
        return key;
    }

    private static String processLength(InputData inputData) {
        String length = null;
        if ("VARCHAR2".equals(inputData.getDataType())) {
            length = String.valueOf(inputData.getCharLength());
        } else if ("NUMBER".equals(inputData.getDataType())) {
            length = String.format("%d,%d", inputData.getDataPrecision(), inputData.getDataScale());
        } else {
            length = String.valueOf(inputData.getDataLength());
        }
        return length;
    }

    private static void writeOutputData(List<OutputData> outputDataList) {
        outputDataList.forEach(outputData -> {
            String outputString = String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s",
                    outputData.getId(),
                    outputData.getKey() == null ? "" : outputData.getKey(),
                    outputData.getColumnName(),
                    outputData.getComments(),
                    outputData.getType(),
                    outputData.getLength(),
                    outputData.getNullable(),
                    outputData.getDefaultValue() == null ? "" : outputData.getDefaultValue(),
                    outputData.getMappingPoColumnName() == null ? "" : outputData.getMappingPoColumnName());
            System.out.println(outputString);
        });
    }

    private static void openWordFile() throws IOException {
        Path tableSchemapath = Paths.get(SVN_PATH);
        List<Path> filePaths = Files.walk(tableSchemapath)
                .filter(Files::isRegularFile)
                .filter(file -> {
                    return file.getFileName().toString().endsWith(TABLE + ".docx");
                }).collect(Collectors.toList());

        if (filePaths.size() == 0) {
            System.err.println(String.format("找不到 Table Schema 文件，請自行到 SVN 開啟"));
        } else if (filePaths.size() > 1) {
            System.err.println(String.format("你有 %d 份 Table Schema 文件，下方開啟第一份!", filePaths.size()));
        }

        Desktop.getDesktop().open(filePaths.get(0).toFile());
    }

    private static void runB() throws ClassNotFoundException {
        Class.forName("oracle.jdbc.OracleDriver");
        try (Connection connection = DriverManager.getConnection(getDbUrl(), DB_USER, DB_USER)) {
            Statement statement = connection.createStatement();
            ResultSet result = statement.executeQuery(getQuerySqlForIndex());
            while (result.next()) {
                String rownum = result.getString("NUM");
                String indexName = result.getString("INDEX_NAME");
                String columns = result.getString("CLOUMNS");
                String uniqueness = result.getString("UNIQUENESS");
                System.out.println(String.format("%s\t%s\t%s\t%s", rownum, indexName, columns, uniqueness));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String getQuerySqlForIndex() {
        return String.format("select rownum AS NUM,\r\n" +
                "       ind.index_name AS INDEX_NAME,\r\n" +
                "       ind.columns AS CLOUMNS,\r\n" +
                "       ind.uniqueness AS UNIQUENESS\r\n" +
                "  from (select index_owner,table_owner,index_name,uniqueness, status,index_type,temporary, partitioned,funcidx_status, join_index,\r\n" +
                "               max(decode(position,1 ,column_name))||\r\n" +
                "               max(decode(position,2 ,', '||column_name))||\r\n" +
                "               max(decode(position,3 ,', '||column_name))||\r\n" +
                "               max(decode(position,4 ,', '||column_name)) columns\r\n" +
                "          from(select di.owner index_owner,dc.table_owner,dc.index_name,di.uniqueness, di.status,\r\n" +
                "                      di.index_type, di.temporary, di.partitioned,di.funcidx_status, di.join_index,\r\n" +
                "                      dc.column_name,dc.column_position position\r\n" +
                "                 from all_ind_columns dc,all_indexes di\r\n" +
                "                where di.table_owner = '%s'\r\n" +
                "                  and di.table_name  =  '%s'\r\n" +
                "                  and dc.index_name = di.index_name\r\n" +
                "                  and dc.index_owner = di.owner\r\n" +
                "              ) group by index_owner,table_owner,index_name,uniqueness, status, index_type, temporary, partitioned,funcidx_status, join_index) ind,\r\n" +
                "                         ALL_IND_EXPRESSIONS ie\r\n" +
                "                where ind.index_name = ie.index_name(+)\r\n" +
                "                  and ind.index_owner = ie.index_owner(+)", SCHEMA, TABLE);
    }

}

package test.ouyang;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class Main {

    private static final String TABLE_OWNER = "TABLE_OWNER";
    private static final String TABLE_NAME = "TABLE_NAME";
    private static final String BEAN_NAME = "BEAN_NAME";

    private static final String DB_HOST = "DB_HOST";
    private static final String DB_PORT = "DB_PORT";
    private static final String DB_SCHEMA = "DB_SCHEMA";
    private static final String DB_USER = "DB_USER";
    private static final String DB_PASSWORD = "DB_PASSWORD";

    public static void main(String[] args) throws ClassNotFoundException {
        Class.forName("oracle.jdbc.OracleDriver");
        try (Connection connection = DriverManager.getConnection(getDbUrl(), DB_USER, DB_PASSWORD)) {
            Statement statement = connection.createStatement();
            ResultSet result = statement.executeQuery(getQuerySql());
            while (result.next()) {
                String columnName = result.getString("COLUMN_NAME");
                String dataType = result.getString("DATA_TYPE");
                String charLength = result.getString("CHAR_LENGTH");
                String nullable = result.getString("NULLABLE");
                System.out.println(
                        String.format(".addValue(\"I_%s\", %s.get%s()) // %s%s", columnName, BEAN_NAME, dbColumnNametoJavaFieldName(columnName), addLengthIfVarChar(dataType, charLength),
                                showNotNull(nullable)));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String getDbUrl() {
        return String.format("jdbc:oracle:thin:@%s:%s/%s", DB_HOST, DB_PORT, DB_SCHEMA);
    }

    private static String getQuerySql() {
        return String.format("select COLUMN_NAME,\r\n" +
                "       DATA_TYPE,\r\n" +
                "       CHAR_LENGTH,\r\n" +
                "       NULLABLE\r\n" +
                "  from ALL_TAB_COLUMNS\r\n" +
                " where OWNER = '%s'\r\n" +
                "   and TABLE_NAME = '%s'", TABLE_OWNER, TABLE_NAME);
    }

    private static String dbColumnNametoJavaFieldName(String columnName) {
        StringBuilder returningString = new StringBuilder();
        String lowerCaseString = columnName.toLowerCase();
        boolean afterUnderscore = false;
        for (int i = 0; i < lowerCaseString.length(); i++) {
            char tmpChar = lowerCaseString.charAt(i);
            if (i == 0 || afterUnderscore) {
                tmpChar = Character.toUpperCase(tmpChar);
                afterUnderscore = false;
            } else if (tmpChar == '_') {
                afterUnderscore = true;
                continue;
            }
            returningString.append(tmpChar);
        }
        return returningString.toString();
    }

    private static String addLengthIfVarChar(String dataType, String charLength) {
        if ("VARCHAR2".equals(dataType)) {
            return String.format("%s(%s)", dataType, charLength);
        }
        return dataType;
    }

    private static String showNotNull(String nullable) {
        if ("N".equals(nullable)) {
            return String.format(", Not NULL");
        }
        return "";
    }

}

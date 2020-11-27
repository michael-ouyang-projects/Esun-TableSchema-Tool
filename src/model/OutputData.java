package model;

public class OutputData {

    private String id;
    private String key;
    private String columnName;
    private String comments;
    private String type;
    private String length;
    private String nullable;
    private String defaultValue;
    private String mappingPoColumnName;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getLength() {
        return length;
    }

    public void setLength(String length) {
        this.length = length;
    }

    public String getNullable() {
        return nullable;
    }

    public void setNullable(String nullable) {
        this.nullable = nullable;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public String getMappingPoColumnName() {
        return mappingPoColumnName;
    }

    public void setMappingPoColumnName(String mappingPoColumnName) {
        this.mappingPoColumnName = mappingPoColumnName;
    }

}

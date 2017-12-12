package de.hpi.isg.mdms.tools.sql;

/**
 * Stores a SQL definition of a primary key.
 */
public class NotNullDefinition {

    private final String tableName;

    private final String columnName;

    public NotNullDefinition(String tableName, String columnName) {
        this.tableName = tableName;
        this.columnName = columnName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final NotNullDefinition that = (NotNullDefinition) o;

        if (tableName != null ? !tableName.equals(that.tableName) : that.tableName != null) return false;
        return columnName != null ? columnName.equals(that.columnName) : that.columnName == null;
    }

    @Override
    public int hashCode() {
        int result = tableName != null ? tableName.hashCode() : 0;
        result = 31 * result + (columnName != null ? columnName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return String.format("%s (%s NOT NULL)", this.tableName, this.columnName);
    }
}

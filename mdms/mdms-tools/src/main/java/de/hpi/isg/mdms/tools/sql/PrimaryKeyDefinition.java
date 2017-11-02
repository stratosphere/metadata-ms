package de.hpi.isg.mdms.tools.sql;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Stores a SQL definition of a primary key.
 */
public class PrimaryKeyDefinition {

    private final String tableName;

    private final List<String > columnNames;

    public PrimaryKeyDefinition(String tableName, List<String> columnNames) {
        this.tableName = tableName;
        this.columnNames = columnNames;
    }

    public PrimaryKeyDefinition(String tableName, String... columnNames) {
        this(tableName, Arrays.asList(columnNames));
    }

    public String getTableName() {
        return this.tableName;
    }

    public List<String> getColumnNames() {
        return this.columnNames;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final PrimaryKeyDefinition that = (PrimaryKeyDefinition) o;
        return Objects.equals(tableName, that.tableName) &&
                Objects.equals(columnNames, that.columnNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, columnNames);
    }

    @Override
    public String toString() {
        return String.format("PRIMARY KEY %s (%s)", this.tableName, this.columnNames.stream().collect(Collectors.joining(", ")));
    }
}

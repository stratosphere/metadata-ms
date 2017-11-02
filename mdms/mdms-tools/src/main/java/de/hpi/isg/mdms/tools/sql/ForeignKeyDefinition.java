package de.hpi.isg.mdms.tools.sql;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Stores a SQL definition of a primary key.
 */
public class ForeignKeyDefinition {

    private final String depTableName, refTableName;

    private final List<String> depColumnNames, refColumnNames;

    public ForeignKeyDefinition(String depTableName,
                                List<String> depColumnNames,
                                String refTableName,
                                List<String> refColumnNames) {
        this.depTableName = depTableName;
        this.depColumnNames = depColumnNames;
        this.refTableName = refTableName;
        this.refColumnNames = refColumnNames;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ForeignKeyDefinition that = (ForeignKeyDefinition) o;
        return Objects.equals(depTableName, that.depTableName) &&
                Objects.equals(refTableName, that.refTableName) &&
                Objects.equals(depColumnNames, that.depColumnNames) &&
                Objects.equals(refColumnNames, that.refColumnNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(depTableName, refTableName, depColumnNames, refColumnNames);
    }

    @Override
    public String toString() {
        return String.format("FOREIGN KEY %s (%s) REFERENCES %s (%s)",
                this.depTableName, this.depColumnNames.stream().collect(Collectors.joining(", ")),
                this.refTableName, this.refColumnNames.stream().collect(Collectors.joining(", "))
        );
    }
}

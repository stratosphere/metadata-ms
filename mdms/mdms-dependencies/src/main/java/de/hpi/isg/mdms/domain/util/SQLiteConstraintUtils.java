package de.hpi.isg.mdms.domain.util;


import de.hpi.isg.mdms.domain.constraints.*;
import de.hpi.isg.mdms.rdbms.SQLiteInterface;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * This class provides utility methods to work with SQLiteInterfaces.
 *
 * @author Sebastian
 * @since 04.03.2015.
 */
public class SQLiteConstraintUtils {


    /**
     * Registers the standard constraint serializers for SQLite with the given SQLite interface.
     * @param sqliteInterface is the SQLiteInterface with that the serializers shall be registered
     * @return the given interface
     */
    public static SQLiteInterface registerStandardConstraints(SQLiteInterface sqliteInterface) {
        sqliteInterface.registerConstraintSQLSerializer(DistinctValueCount.class,
                new DistinctValueCount.DistinctValueCountSQLiteSerializer(sqliteInterface));
        sqliteInterface.registerConstraintSQLSerializer(InclusionDependency.class,
                new InclusionDependency.InclusionDependencySQLiteSerializer(sqliteInterface));
        sqliteInterface.registerConstraintSQLSerializer(FunctionalDependency.class,
                new FunctionalDependency.FunctionalDependencySQLiteSerializer(sqliteInterface));
        sqliteInterface.registerConstraintSQLSerializer(TupleCount.class, new TupleCount.TupleCountSQLiteSerializer(
                sqliteInterface));
        sqliteInterface.registerConstraintSQLSerializer(TypeConstraint.class,
                new TypeConstraint.TypeConstraintSQLiteSerializer(sqliteInterface));
        sqliteInterface.registerConstraintSQLSerializer(UniqueColumnCombination.class, new
                UniqueColumnCombination.UniqueColumnCombinationSQLiteSerializer(sqliteInterface));
        sqliteInterface.registerConstraintSQLSerializer(DistinctValueOverlap.class, new
                DistinctValueOverlap.DistinctValueOverlapSQLiteSerializer(sqliteInterface));
        sqliteInterface.registerConstraintSQLSerializer(ColumnStatistics.class,
                new ColumnStatistics.SQLiteSerializer(sqliteInterface));
        sqliteInterface.registerConstraintSQLSerializer(TextColumnStatistics.class,
                new TextColumnStatistics.SQLiteSerializer(sqliteInterface));
        sqliteInterface.registerConstraintSQLSerializer(NumberColumnStatistics.class,
                new NumberColumnStatistics.SQLiteSerializer(sqliteInterface));


        return sqliteInterface;
    }

    /**
     * Loads the value for the given column from a {@link ResultSet}. If the value was {@code null}, a fallback
     * value is instead returned.
     *
     * @param resultSet to read from
     * @param column    the column whose value is requested
     * @param fallback  the fallback value
     * @return the value in the {@code resultSet} for the {@code column}, or the {@code fallback} value if it
     * was {@code null}
     * @throws SQLException
     */
    public static double getNullableDouble(ResultSet resultSet, String column, double fallback) throws SQLException {
        double value = resultSet.getDouble(column);
        return (value == 0d && resultSet.wasNull()) ? fallback : value;
    }

    /**
     * Loads the value for the given column from a {@link ResultSet}. If the value was {@code null}, a fallback
     * value is instead returned.
     *
     * @param resultSet to read from
     * @param column    the column whose value is requested
     * @param fallback  the fallback value
     * @return the value in the {@code resultSet} for the {@code column}, or the {@code fallback} value if it
     * was {@code null}
     * @throws SQLException
     */
    public static int getNullableInt(ResultSet resultSet, String column, int fallback) throws SQLException {
        int value = resultSet.getInt(column);
        return (value == 0 && resultSet.wasNull()) ? fallback : value;
    }

}


package de.hpi.isg.mdms.domain.util;


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


package de.hpi.isg.mdms.db;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface PreparedStatementAdapter<T> {

    void translateParameter(T object, PreparedStatement preparedStatement) throws SQLException;

    /**
     * {@link de.hpi.isg.mdms.db.PreparedStatementAdapter} for queries that have a single integer parameter.
     */
    PreparedStatementAdapter<Integer> SINGLE_INT_ADAPTER =
            (integer, preparedStatement) -> preparedStatement.setInt(1, integer);

    /**
     * {@link de.hpi.isg.mdms.db.PreparedStatementAdapter} for queries that have a single {@link String} parameter.
     */
    PreparedStatementAdapter<String> SINGLE_STRING_ADAPTER =
            (str, preparedStatement) -> preparedStatement.setString(1, str);

    PreparedStatementAdapter<Void> VOID_ADAPTER =
            (object, preparedStatement) -> {
            };

}
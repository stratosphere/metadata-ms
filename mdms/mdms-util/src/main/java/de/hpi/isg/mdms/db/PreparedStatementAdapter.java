package de.hpi.isg.mdms.db;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface PreparedStatementAdapter<T> {

    void translateParameter(T object, PreparedStatement preparedStatement) throws SQLException;

    /**
     * {@link de.hpi.isg.mdms.db.PreparedStatementAdapter} for queries that have a single integer parameter.
     */
    static final PreparedStatementAdapter<Integer> SINGLE_INT_ADAPTER =
            new PreparedStatementAdapter<Integer>() {

                public void translateParameter(Integer integer, PreparedStatement preparedStatement)
                        throws SQLException {
                    preparedStatement.setInt(1, integer);
                }
            };

    static final PreparedStatementAdapter<Void> VOID_ADAPTER =
            new PreparedStatementAdapter<Void>() {

                @Override
                public void translateParameter(Void object, PreparedStatement preparedStatement) throws SQLException {

                }
            };

}
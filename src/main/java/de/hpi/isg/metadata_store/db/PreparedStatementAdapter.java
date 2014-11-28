package de.hpi.isg.metadata_store.db;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface PreparedStatementAdapter<T> {

	void translateParameter(T object, PreparedStatement preparedStatement) throws SQLException;

	/**
	 * {@link PreparedStatementAdapter} for queries that have a single integer parameter.
	 */
	static final PreparedStatementAdapter<Integer> SINGLE_INT_ADAPTER =
			new PreparedStatementAdapter<Integer>() {

				public void translateParameter(Integer integer, PreparedStatement preparedStatement) throws SQLException {
					preparedStatement.setInt(1, integer);
				}
			};

}
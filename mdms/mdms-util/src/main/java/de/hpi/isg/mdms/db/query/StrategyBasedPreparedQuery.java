package de.hpi.isg.mdms.db.query;

import java.sql.SQLException;

import de.hpi.isg.mdms.db.DatabaseAccess;
import de.hpi.isg.mdms.db.PreparedStatementAdapter;

public class StrategyBasedPreparedQuery<T> extends PreparedQuery<T> {

	private final PreparedStatementAdapter<T> preparedStatementAdapter;

	public StrategyBasedPreparedQuery(DatabaseAccess databaseAccess, String sql, PreparedStatementAdapter<T> preparedStatementAdapter,
			String... queriedTables) {

		super(databaseAccess, sql, queriedTables);
		this.preparedStatementAdapter = preparedStatementAdapter;
	}

	@Override
	protected void setStatementParameters(T element) throws SQLException {
		this.preparedStatementAdapter.translateParameter(element, this.preparedStatement);
	}

	public static class Factory<TElement> implements DatabaseQuery.Factory<StrategyBasedPreparedQuery<TElement>> {

		private final String sqlStatement;

		private final PreparedStatementAdapter<TElement> adapter;

		private String[] queriedTables;

		public Factory(String sqlStatement, PreparedStatementAdapter<TElement> adapter, String... queriedTables) {
			this.sqlStatement = sqlStatement;
			this.adapter = adapter;
			this.queriedTables = queriedTables;
		}

		@Override
		public StrategyBasedPreparedQuery<TElement> createQuery(DatabaseAccess databaseAccess) {
			return new StrategyBasedPreparedQuery<>(databaseAccess,
					this.sqlStatement, this.adapter, this.queriedTables);
		}

	}

}

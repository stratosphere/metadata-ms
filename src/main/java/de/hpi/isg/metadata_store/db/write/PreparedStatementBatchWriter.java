package de.hpi.isg.metadata_store.db.write;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import de.hpi.isg.metadata_store.db.DatabaseAccess;

public class PreparedStatementBatchWriter<T> extends BatchWriter<T> {

	/**
	 * Stores elements as batches to the prepared statement.
	 */
	private final PreparedStatementAdapter<T> adapter;

	public PreparedStatementBatchWriter(Statement statement, DatabaseAccess databaseAccess, Collection<String> referencedTables,
			Collection<String> manipulatedTables, int batchSize, PreparedStatementAdapter<T> adapter) {

		super(statement, databaseAccess, referencedTables, manipulatedTables, batchSize);
		this.adapter = adapter;
	}

	@Override
	protected void addBatch(T element) throws SQLException {
		this.adapter.addBatch(element, (PreparedStatement) this.statement);
	}

	public interface PreparedStatementAdapter<T> {

		void addBatch(T object, PreparedStatement preparedStatement) throws SQLException;

	}

	public static class Factory<TElement> implements DatabaseWriter.Factory<PreparedStatementBatchWriter<TElement>> {

		private final String sqlStatement;

		private final PreparedStatementAdapter<TElement> adapter;

		private Collection<String> manipulatedTables;

		private Collection<String> referencedTables;

		public Factory(String sqlStatement, PreparedStatementAdapter<TElement> adapter, String manipulatedTable, String... referencedTables) {
			this.sqlStatement = sqlStatement;
			this.adapter = adapter;
			this.manipulatedTables = Collections.singleton(manipulatedTable);
			this.referencedTables = Arrays.asList(referencedTables);
		}

		@Override
		public PreparedStatementBatchWriter<TElement> createWriter(DatabaseAccess databaseAccess) throws SQLException {
			Connection connection = databaseAccess.getConnection();
			PreparedStatement preparedStatement = connection.prepareStatement(this.sqlStatement);

			return new PreparedStatementBatchWriter<TElement>(preparedStatement, databaseAccess,
					this.referencedTables, this.manipulatedTables, 
					DEFAULT_BATCH_SIZE, this.adapter);
		}

	}
}

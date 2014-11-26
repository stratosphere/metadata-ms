package de.hpi.isg.metadata_store.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.Validate;

import de.hpi.isg.metadata_store.db.write.BatchWriter;
import de.hpi.isg.metadata_store.db.write.DatabaseWriter;
import de.hpi.isg.metadata_store.db.write.DependentWriter;
import de.hpi.isg.metadata_store.db.write.SQLExecutor;

/**
 * Manages the access to a database by maintaining batch writers and ensuring all data is written before performing a
 * read.
 * 
 * @author Sebastian Kruse
 * 
 */
public class DatabaseAccess implements AutoCloseable {

	/**
	 * The connection to the database.
	 */
	private Connection connection;

	/**
	 * A set of writers that operate on specific tables, usually {@link BatchWriter}s.
	 */
	private Map<String, List<DependentWriter<?>>> writers = new HashMap<>();

	/**
	 * Executes plain SQL statements.
	 */
	private SQLExecutor sqlExecutor;

	// TODO: Query objects.

	public DatabaseAccess(Connection connection) {
		super();
		try {
			Validate.isTrue(!connection.isClosed());
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		this.connection = connection;
		try {
			this.sqlExecutor = new SQLExecutor(connection, this, BatchWriter.DEFAULT_BATCH_SIZE);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public <TWriter extends BatchWriter<?>> TWriter createBatchWriter(DatabaseWriter.Factory<TWriter> factory)
			throws SQLException {

		TWriter writer = factory.createWriter(this);
		for (String manipulatedTable : writer.getManipulatedTables()) {
			List<DependentWriter<?>> list = this.writers.get(manipulatedTable);
			if (list == null) {
				list = new LinkedList<>();
				this.writers.put(manipulatedTable, list);
			}
			list.add(writer);
//			CollectionUtils.putIntoList(this.writers, manipulatedTable, writer);
		}
		return writer;
	}
	
	public void executeSQL(String sqlStmt, String manipulatedTable, String... referencedTables) 
			throws SQLException {
		
		this.sqlExecutor.write(sqlStmt, manipulatedTable, referencedTables);
	}
	
	public ResultSet query(String sql, String... queriedTables) throws SQLException {
		flush(Arrays.asList(queriedTables));
		return this.connection.createStatement().executeQuery(sql);
	}

	/**
	 * Flushes all writers.
	 * 
	 * @throws SQLException
	 *             if the flushing fails for any writer.
	 */
	public void flush() throws SQLException {
		this.sqlExecutor.flush();
		for (Collection<DependentWriter<?>> writers : this.writers.values()) {
			for (DatabaseWriter<?> writer : writers) {
				writer.flush();
			}
		}
	}

	/**
	 * Flushes (at least) all writers that operate on the given tables.
	 * 
	 * @param referencedTables
	 *            are the tables for which writers shall be flushed.
	 * @throws SQLException
	 *             if the flushing fails for any of the writers.
	 */
	public void flush(Collection<String> tables) throws SQLException {
		for (String table : tables) {
			if (this.sqlExecutor.getManipulatedTables().contains(table)) {
				this.sqlExecutor.flush();
			}
			Collection<DependentWriter<?>> writers = this.writers.get(table);
			if (writers == null) {
				continue;
			}
			for (DatabaseWriter<?> writer : writers) {
				writer.flush();
			}
		}
	}
	
	/**
	 * Flushes any pending changes and closes the connection.
	 * 
	 * @throws SQLException if the flushing or closing fails 
	 */
	public void close() throws SQLException {
		try {
			flush();
		} finally {
			this.connection.close();
		}
	}

	/**
	 * @return the database connection that is managed by this object.
	 */
	public Connection getConnection() {
		return this.connection;
	}

}

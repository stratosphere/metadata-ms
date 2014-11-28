package de.hpi.isg.metadata_store.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.Validate;

import de.hpi.isg.metadata_store.db.query.DatabaseQuery;
import de.hpi.isg.metadata_store.db.query.SQLQuery;
import de.hpi.isg.metadata_store.db.query.StrategyBasedPreparedQuery.Factory;
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
	 * Executes plain SQL INSERT/UPDATE statements.
	 */
	private SQLExecutor sqlExecutor;

	/**
	 * (Probably prepared) queries that are available in the database access.
	 */
	private Collection<DatabaseQuery<?>> queries = new LinkedList<>();

	/**
	 * Executes plain SQL SELECT statements.
	 */
	private SQLQuery sqlQuery;

	/**
	 * A mapping from tables to referenced tables (via foreign keys).
	 */
	private Map<String, Set<String>> foreignKeyDependencies = new HashMap<>();

	public DatabaseAccess(Connection connection) {
		super();
		try {
			Validate.isTrue(!connection.isClosed());
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		this.connection = connection;
		try {
			this.sqlExecutor = new SQLExecutor(this, BatchWriter.DEFAULT_BATCH_SIZE);
			this.sqlQuery = new SQLQuery(this);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Loads the foreign keys from the RDBMS.
	 */
	public Set<String> getReferencedTables(String table) {
		Set<String> referencedTables = this.foreignKeyDependencies.get(table);
		if (referencedTables != null) {
			return referencedTables;
		}
		try {
			referencedTables = new HashSet<String>();
			DatabaseMetaData metaData = this.connection.getMetaData();
			ResultSet resultSet = metaData.getImportedKeys(null, null, table);
			while (resultSet.next()) {
				String referencedTable = resultSet.getString("PKTABLE_NAME");
				referencedTables.add(referencedTable);
			}
			resultSet.close();
			this.foreignKeyDependencies.put(table, referencedTables);
			return referencedTables;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public <TWriter extends BatchWriter<?>> TWriter createBatchWriter(DatabaseWriter.Factory<TWriter> factory)
			throws SQLException {

		TWriter writer = factory.createWriter(this);
		for (String manipulatedTable : writer.getManipulatedTables()) {
			getReferencedTables(manipulatedTable);
			List<DependentWriter<?>> list = this.writers.get(manipulatedTable);
			if (list == null) {
				list = new LinkedList<>();
				this.writers.put(manipulatedTable, list);
			}
			list.add(writer);
			// CollectionUtils.putIntoList(this.writers, manipulatedTable, writer);
		}
		return writer;
	}

	/**
	 * Executes a SQL statement on the managed database. Thereby, dependencies to other query batches are respected.
	 * 
	 * @param sqlStmt
	 *            is the SQL statement to execute
	 * @param manipulatedTable
	 *            is the table that is manipulated by this query
	 * @param referencedTables
	 *            are the affecting/affected referenced tables of this query. If no referenced tables are passed, they
	 *            are deduced from the foreign key relationships of the database.
	 * @throws SQLException
	 * @deprecated Use {@link #executeSQL(String, String...)} instead.
	 */
	public void executeSQLWithReferencedTables(String sqlStmt, String manipulatedTable, String... referencedTables)
			throws SQLException {

		referencedTables = updateReferencedTablesIfNotGiven(manipulatedTable, referencedTables);
		this.sqlExecutor.write(sqlStmt, new String[] { manipulatedTable }, referencedTables);
	}

	/**
	 * Executes a SQL statement on the managed database. Thereby, dependencies to other query batches are respected.
	 * 
	 * @param sqlStmt
	 *            is the SQL statement to execute
	 * @param manipulatedTables
	 *            are the tables that are manipulated by this query
	 * @throws SQLException
	 */
	public void executeSQL(String sqlStmt, String... manipulatedTables)
			throws SQLException {

		// TODO: the flush order might not be well-suited for deletions. Handle that differently?
		String[] referencedTables = new String[0];
		for (String manipulatedTable : manipulatedTables) {
			referencedTables = updateReferencedTablesIfNotGiven(manipulatedTable, referencedTables);

		}
		this.sqlExecutor.write(sqlStmt, manipulatedTables, referencedTables);
	}

	private String[] updateReferencedTablesIfNotGiven(String manipulatedTable, String... referencedTables) {
		if (referencedTables.length == 0) {
			Set<String> fkReferencedTables = getReferencedTables(manipulatedTable);
			referencedTables = fkReferencedTables.toArray(new String[fkReferencedTables.size()]);
		} else {
			Logger.getGlobal().warning(String.format("Manually passed referenced tables detected: %s: %s.",
					manipulatedTable, Arrays.toString(referencedTables)));
		}
		return referencedTables;
	}

	public ResultSet query(String sql, String... queriedTables) throws SQLException {
		flush(Arrays.asList(queriedTables));
		return this.sqlQuery.execute(sql, queriedTables);
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
		Logger.getGlobal().log(Level.INFO, String.format("Flushing modifications on %s.\n", tables));
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
	 * @throws SQLException
	 *             if the flushing or closing fails
	 */
	public void close() throws SQLException {
		try {
			flush();
		} finally {
			tryToClose(this.sqlExecutor);
			tryToClose(this.sqlQuery);
			for (List<DependentWriter<?>> writers : this.writers.values()) {
				for (DependentWriter<?> writer : writers) {
					tryToClose(writer);
				}
			}
			writers.clear();
			for (DatabaseQuery<?> query : this.queries) {
				tryToClose(query);
			}
			this.queries.clear();
			this.connection.close();
		}
	}

	/** Closes an {@link AutoCloseable} if not {@code null} and catches any exceptions. */
	private void tryToClose(AutoCloseable closeable) {
		if (closeable == null) {
			return;
		}
		try {
			closeable.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * @return the database connection that is managed by this object.
	 */
	public Connection getConnection() {
		return this.connection;
	}

	public <TElement> DatabaseQuery<TElement> createQuery(Factory<TElement> locationQueryFactory) {
		return locationQueryFactory.createQuery(this);
	}

}

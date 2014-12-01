package de.hpi.isg.metadata_store.db.write;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import de.hpi.isg.metadata_store.db.DatabaseAccess;

/**
 * A batch writer manages inserts into a database. Thereby, it can bundle multiple actions into batches.
 * 
 * @author Sebastian Kruse
 *
 */
abstract public class DatabaseWriter<T> implements AutoCloseable {
	
	protected final Connection connection;
	
	/**
	 * The statement over which SQL queries are issued; lazy-initialized.
	 */
	protected Statement statement;
	
	protected DatabaseWriter(Connection connection) {
		super();
		this.connection = connection;
	}

	public void write(T element) throws SQLException {
		ensureStatementInitialized();
		doWrite(element);
	}

	/**
	 * Ensures that {@link #statement} is properly set up for write operation.
	 * 
	 * @throws SQLException
	 */
	protected abstract void ensureStatementInitialized() throws SQLException;

	abstract protected void doWrite(T element) throws SQLException;
	
	public void flush() throws SQLException {
		if (this.statement != null) {
//			Logger.getGlobal().log(Level.INFO, String.format("Flushing %s.", this));
			doFlush();
		}
	}

	abstract protected void doFlush() throws SQLException;
	
	public void close() throws SQLException {
		try {
			flush();
		} finally {
			this.statement.close();
			this.statement = null;
		}
	}

	/**
	 * A {@link Factory} should be used to create {@link DatabaseWriter} objects. 
	 * 
	 * @author Sebastian Kruse
	 *
	 * @param <TWriter> is the type of writer created by this factory
	 */
	public static interface Factory<TWriter extends DatabaseWriter<?>> {
		
		/**
		 * Creates a new writer on the given connection.
		 * @param connection is the database access 
		 * @return
		 * @throws SQLException
		 */
		TWriter createWriter(DatabaseAccess databaseAccess) throws SQLException;
		
	}
}

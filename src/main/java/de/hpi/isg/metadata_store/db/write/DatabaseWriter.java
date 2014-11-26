package de.hpi.isg.metadata_store.db.write;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * A batch writer manages inserts into a database. Thereby, it can bundle multiple actions into batches.
 * 
 * @author Sebastian Kruse
 *
 */
abstract public class DatabaseWriter<T> {
	
	protected final Statement statement;
	
	protected DatabaseWriter(Statement statement) {
		super();
		this.statement = statement;
	}

	abstract void write(T element) throws SQLException;
	
	abstract public void flush() throws SQLException;
	
	public void close() throws SQLException {
		try {
			flush();
		} finally {
			this.statement.close();
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
		TWriter createWriter(Connection connection) throws SQLException;
		
	}
}

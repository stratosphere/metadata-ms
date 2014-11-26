package de.hpi.isg.metadata_store.db.write;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;

import de.hpi.isg.metadata_store.db.DatabaseAccess;

/**
 * This {@link DatabaseWriter} can be used to execute generic SQL statements.
 * 
 * @author Sebastian Kruse
 * 
 */
public class SQLExecutor extends BatchWriter<String> {

	public SQLExecutor(Connection connection, DatabaseAccess databaseAccess,
			Collection<String> referencedTables, Collection<String> manipulatedTables,
			int batchSize) throws SQLException {

		super(connection.createStatement(), databaseAccess, referencedTables, manipulatedTables, batchSize);
	}

	public void write(String element, String manipulatedTable, String... referencedTables) throws SQLException {
		this.manipulatedTables.add(manipulatedTable);
		for (String referencedTable : referencedTables) {
			this.referencedTables.add(referencedTable);
		}
		super.write(element);
	}
	
	@Override
	public void write(String element) throws SQLException {
		throw new UnsupportedOperationException("Used #write(String, String, String...) instead.");
	}
	
	@Override
	protected void addBatch(String sql) throws SQLException {
		this.statement.addBatch(sql);
	}
	
	@Override
	public void flush() throws SQLException {
		super.flush();
		this.manipulatedTables.clear();
		this.referencedTables.clear();
	}

}

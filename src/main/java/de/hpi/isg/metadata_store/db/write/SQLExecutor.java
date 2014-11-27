package de.hpi.isg.metadata_store.db.write;

import java.sql.SQLException;
import java.util.Collections;

import de.hpi.isg.metadata_store.db.DatabaseAccess;

/**
 * This {@link DatabaseWriter} can be used to execute generic SQL statements.
 * 
 * @author Sebastian Kruse
 * 
 */
public class SQLExecutor extends BatchWriter<String> {

	public SQLExecutor(DatabaseAccess databaseAccess,
			int batchSize) throws SQLException {

		super(databaseAccess, 
				Collections.<String>emptySet(), 
				batchSize);
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
	protected void ensureStatementInitialized() throws SQLException {
		if (this.statement == null) {
			this.statement = this.connection.createStatement();
		}
	}

	@Override
	protected void addBatch(String sql) throws SQLException {
		this.statement.addBatch(sql);
	}
	
	@Override
	protected void doFlush() throws SQLException {
		super.doFlush();
		this.manipulatedTables.clear();
		this.referencedTables.clear();
	}

}

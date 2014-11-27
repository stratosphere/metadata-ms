package de.hpi.isg.metadata_store.db.write;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

import de.hpi.isg.metadata_store.db.DatabaseAccess;

/**
 * This class allows to bundle writes of a {@link DatabaseWriter} in batches of a certain size.
 *
 * @author Sebastian Kruse
 *
 * @param <T>
 */
public abstract class BatchWriter<T> extends DependentWriter<T> {

	public static final int DEFAULT_BATCH_SIZE = 0;
	
	/**
	 * The maximum number of SQL statements to include in a batch.
	 */
	private int maxBatchSize;
	
	/**
	 * The number of SQL statements in the current batch.
	 */
	private int curBatchSize;

	/**
	 * Creates a new {@link BatchWriter}.
	 * @param databaseAccess see {@link DependentWriter#DependentWriter(Statement, DatabaseAccess, Collection, Collection)}
	 * @param referencedTables see {@link DependentWriter#DependentWriter(Statement, DatabaseAccess, Collection, Collection)}
	 * @param manipulatedTables see {@link DependentWriter#DependentWriter(Statement, DatabaseAccess, Collection, Collection)}
	 * @param batchSize is the maximum number of statements to execute in a single batch
	 */
	public BatchWriter(DatabaseAccess databaseAccess, 
			Collection<String> manipulatedTables, int batchSize) {
		
		super(databaseAccess, manipulatedTables);
		this.maxBatchSize = batchSize;
		this.curBatchSize = 0;
	}
	
	@Override
	public void doWrite(T element) throws SQLException {
		addBatch(element);
		if (++this.curBatchSize >= this.maxBatchSize) {
			flush();
		}
	}

	abstract protected void addBatch(T element) throws SQLException;
	
	@Override
	protected void doFlush() throws SQLException {
		if (this.curBatchSize > 0) {
			ensureReferencedTablesFlushed();
			int[] batchResults = this.statement.executeBatch();
			for (int result : batchResults) {
				if (result == Statement.EXECUTE_FAILED) {
					throw new SQLException("Batch execution returned error on one or more SQL statements.");
				}
			}
		}
		this.curBatchSize = 0;
	}

}

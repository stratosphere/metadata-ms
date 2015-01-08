package de.hpi.isg.metadata_store.db.write;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.hpi.isg.metadata_store.db.DatabaseAccess;

/**
 * This class allows to bundle writes of a {@link DatabaseWriter} in batches of a certain size.
 *
 * @author Sebastian Kruse
 *
 * @param <T>
 */
public abstract class BatchWriter<T> extends DependentWriter<T> {
    
    private final static Logger LOGGER = LoggerFactory.getLogger(BatchWriter.class);

	public static final int DEFAULT_BATCH_SIZE = 10000;
	
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
	 * @param accessedTables see {@link DependentWriter#DependentWriter(Statement, DatabaseAccess, Collection, Collection)}
	 * @param manipulatedTables see {@link DependentWriter#DependentWriter(Statement, DatabaseAccess, Collection, Collection)}
	 * @param batchSize is the maximum number of statements to execute in a single batch
	 */
	public BatchWriter(DatabaseAccess databaseAccess,
	        Collection<String> accessedTables,
			Collection<String> manipulatedTables, int batchSize) {
		
		super(databaseAccess, accessedTables, manipulatedTables);
		this.maxBatchSize = batchSize;
		this.curBatchSize = 0;
	}
	
	@Override
	public void doWrite(T element) throws SQLException {
		fireAboutToAddBatchElement();
		addBatch(element);
		if (++this.curBatchSize >= this.maxBatchSize) {
		    flush();
		}
	}

	abstract protected void addBatch(T element) throws SQLException;
	
	@Override
	protected void doFlush() throws SQLException {
		if (this.curBatchSize > 0) {
		    int batchSize = this.curBatchSize;
		    long startTime = System.currentTimeMillis();
			int[] batchResults = this.statement.executeBatch();
			if (!this.statement.getConnection().getAutoCommit()) {
			    this.statement.getConnection().commit();
			}
			for (int result : batchResults) {
				if (result == Statement.EXECUTE_FAILED) {
					throw new SQLException("Batch execution returned error on one or more SQL statements.");
				}
			}
			long endTime = System.currentTimeMillis();
			LOGGER.debug("Flushed {} statements from {} in {} ms ", batchSize, this, endTime - startTime);
		} else {
		    LOGGER.warn("Attempted to flush empty batch writer {}.", this);
		}
		this.curBatchSize = 0;
	}
	
	/** Called when the batch was empty but is not anymore. */
	protected void fireAboutToAddBatchElement() {
	    // With queries in the batch, data dependencies of this writer become relevant.
		ensureReferencedTablesDetermined();
	    if (!this.manipulatedTables.isEmpty() || !this.accessedTables.isEmpty()) {
	        this.databaseAccess.notifyWriterAction(this, this.manipulatedTables, this.accessedTables);
	    }
	}
	
	// TODO Delete method code.
//	/** Called when the batch was flushed. */
//	private void fireBatchFlushed() {
//	    // Without queries in the batch, this writer is neutral wrt. accessed and modified tables.
//	    if (!this.manipulatedTables.isEmpty() || !this.accessedTables.isEmpty()) {
//	        this.databaseAccess.notifyTablesClear(this);
//	    }
//	}
	
	@Override
	public Set<String> getManipulatedTables() {
	    if (this.curBatchSize == 0) {
	        return Collections.emptySet();
	    }
	    return super.getManipulatedTables();
	}
	
	@Override
	public Set<String> getAccessedTables() {
	    if (this.curBatchSize == 0) {
	        return Collections.emptySet();
	    }
	    return super.getAccessedTables();
	}
	

}

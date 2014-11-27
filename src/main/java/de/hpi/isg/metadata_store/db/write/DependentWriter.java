package de.hpi.isg.metadata_store.db.write;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import de.hpi.isg.metadata_store.db.DatabaseAccess;

/**
 * A {@link DependentWriter} inserts/updates tuples that reference tuples of other tables. A {@link DatabaseAccess}
 * object can manage these dependencies. The {@link DependentWriter} offers utility methods to manage its own
 * dependencies and let the {@link DatabaseAccess} take care of enforcing referential integrity.
 * 
 * @author Sebastian Kruse
 * 
 */
abstract public class DependentWriter<T> extends DatabaseWriter<T> {

	/**
	 * A {@link DatabaseAccess} that manages dependencies among writers.
	 */
	private DatabaseAccess databaseAccess;

	/**
	 * The names of the tables that are referenced by the manipulated tables.
	 */
	protected Set<String> referencedTables;

	/**
	 * The names of the tables that are manipulated by this writer or current batch.
	 */
	protected Set<String> manipulatedTables;
	
	public static Collection<String> findAllReferencedTables(Collection<String> manipulatedTables, DatabaseAccess databaseAccess) {
	    Collection<String> allReferencedTables = new LinkedList<>();
	    for (String manipulatedTable : manipulatedTables) {
	        Set<String> referencedTables = databaseAccess.getReferencedTables(manipulatedTable);
	        allReferencedTables.addAll(referencedTables);
	    }
	    return allReferencedTables;
	}
	

	public DependentWriter(DatabaseAccess databaseAccess, Collection<String> manipulatedTables) {
	    this(databaseAccess, findAllReferencedTables(manipulatedTables, databaseAccess), manipulatedTables);
	    
	}
	
	@Deprecated
	public DependentWriter(DatabaseAccess databaseAccess, Collection<String> referencedTables, Collection<String> manipulatedTables) {
		super(databaseAccess.getConnection());

		this.databaseAccess = databaseAccess;
		this.referencedTables = new HashSet<>(referencedTables);
		this.manipulatedTables = new HashSet<>(manipulatedTables);
	}
	
	protected void ensureReferencedTablesFlushed() throws SQLException {
		if (!this.referencedTables.isEmpty()) {
			this.databaseAccess.flush(this.referencedTables);
		}
	}

	public Set<String> getManipulatedTables() {
		return manipulatedTables;
	}
	
	public Set<String> getReferencedTables() {
		return referencedTables;
	}
}

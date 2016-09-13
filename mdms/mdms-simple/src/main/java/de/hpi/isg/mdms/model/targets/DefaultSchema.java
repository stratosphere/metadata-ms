package de.hpi.isg.mdms.model.targets;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.Validate;

import de.hpi.isg.mdms.model.location.Location;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.common.Observer;
import de.hpi.isg.mdms.model.common.ExcludeHashCodeEquals;
import de.hpi.isg.mdms.exceptions.NameAmbigousException;

/**
 * The default implementation of the {@link de.hpi.isg.mdms.model.targets.Schema}.
 *
 */
public class DefaultSchema extends AbstractTarget implements Schema {

    public static Schema buildAndRegister(final Observer observer, final int id, final String name,
            String description, final Location location) {
        final DefaultSchema newSchema = new DefaultSchema(observer, id, name, description, location);
        newSchema.register();
        return newSchema;
    }

    public static Schema buildAndRegister(final Observer observer, final String name, final String description,
            final Location location) {
        final DefaultSchema newSchema = new DefaultSchema(observer, -1, name, description, location);
        newSchema.register();
        return newSchema;
    }

    private static final long serialVersionUID = 8383281581697630605L;

    @ExcludeHashCodeEquals
    private final Collection<Table> tables;

    private DefaultSchema(final Observer observer, final int id, final String name, final String description,
            final Location location) {
        super(observer, id, name, description, location);
        this.tables = Collections.synchronizedSet(new HashSet<Table>());
    }

    @Override
    public Table addTable(final MetadataStore metadataStore, final String name, final String description,
            final Location location) {
        Validate.isTrue(metadataStore.getSchemas().contains(this));
        final int tableId = metadataStore.getUnusedTableId(this);
        final Table table = DefaultTable.buildAndRegister(metadataStore, this, tableId, name, description, location);
        this.tables.add(table);
        return table;
    }

    @Override
    public Table getTableByName(final String name) throws NameAmbigousException {
        final List<Table> results = new ArrayList<>();
        for (final Table table : this.tables) {
            if (table.getName().equals(name)) {
                results.add(table);
            }
        }
        if (results.size() > 1) {
            throw new NameAmbigousException(name);
        }
        if (results.isEmpty()) {
            return null;
        }
        return results.get(0);
    }

    @Override
    public Collection<Table> getTables() {
        return Collections.unmodifiableCollection(this.tables);
    }

    @Override
    public Column findColumn(final int columnId) {
        for (final Table table : getTables()) {
            for (final Column column : table.getColumns()) {
                if (column.getId() == columnId) {
                    return column;
                }
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return String.format("Schema[%s, %d tables, %08x]", this.getName(), this.getTables().size(), this.getId());
    }

    @Override
    public Collection<Table> getTablesByName(String name) {
        return this.tables
                .stream()
                .filter(table -> table.getName().equals(name))
                .collect(Collectors.toList());
    }

    @Override
    public Table getTableById(int id) {
        for (Table table : this.tables) {
            if (table.getId() == id) {
                return table;
            }
        }

        throw new NoSuchElementException("No Table found with the id " + id);
    }
}

/***********************************************************************************************************************
 * Copyright (C) 2014 by Sebastian Kruse
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/
package de.hpi.isg.metadata_store.domain;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import de.hpi.isg.metadata_store.domain.impl.DefaultMetadataStore;
import de.hpi.isg.metadata_store.domain.util.IdUtils;

public class IdUtilsTest {

    private static final boolean VERBOSE = true;

    @Test
    public void testIdAssembling() {
        final List<Integer> schemaIds = Arrays.asList(IdUtils.MIN_SCHEMA_NUMBER, IdUtils.MIN_SCHEMA_NUMBER + 1,
                IdUtils.MAX_SCHEMA_NUMBER - 1, IdUtils.MAX_SCHEMA_NUMBER);
        final List<Integer> tableIds = Arrays.asList(IdUtils.MIN_TABLE_NUMBER, IdUtils.MIN_TABLE_NUMBER + 1,
                IdUtils.MAX_TABLE_NUMBER - 1, IdUtils.MAX_TABLE_NUMBER);
        final List<Integer> columnIds = Arrays.asList(IdUtils.MIN_COLUMN_NUMBER, IdUtils.MIN_COLUMN_NUMBER + 1,
                IdUtils.MAX_COLUMN_NUMBER - 1, IdUtils.MAX_COLUMN_NUMBER);
        for (final int schemaId : schemaIds) {
            for (final int tableId : tableIds) {
                for (final int columnId : columnIds) {
                    final int globalId = IdUtils.createGlobalId(schemaId, tableId, columnId);

                    if (VERBOSE) {
                        System.out.format("[%s] (%3d, %4d, %4d) -> %9x\n", getClass().getSimpleName(), schemaId,
                                tableId, columnId, globalId);
                    }

                    Assert.assertEquals(schemaId, IdUtils.getLocalSchemaId(globalId));
                    Assert.assertEquals(tableId, IdUtils.getLocalTableId(globalId));
                    Assert.assertEquals(columnId, IdUtils.getLocalColumnId(globalId));
                }
            }
        }
    }

    @Test
    public void testTableIdsHaveNoValidColumn() {
        final List<Integer> schemaIds = Arrays.asList(IdUtils.MIN_SCHEMA_NUMBER, IdUtils.MIN_SCHEMA_NUMBER + 1,
                IdUtils.MAX_SCHEMA_NUMBER - 1, IdUtils.MAX_SCHEMA_NUMBER);
        final List<Integer> tableIds = Arrays.asList(IdUtils.MIN_TABLE_NUMBER, IdUtils.MIN_TABLE_NUMBER + 1,
                IdUtils.MAX_TABLE_NUMBER - 1, IdUtils.MAX_TABLE_NUMBER);
        for (final int schemaId : schemaIds) {
            for (final int tableId : tableIds) {
                final int globalId = IdUtils.createGlobalId(schemaId, tableId);

                if (VERBOSE) {
                    System.out.format("[%s] (%3d, %4d, ----) -> %9x\n", getClass().getSimpleName(), schemaId,
                            tableId, globalId);
                }

                Assert.assertEquals(schemaId, IdUtils.getLocalSchemaId(globalId));
                Assert.assertEquals(tableId, IdUtils.getLocalTableId(globalId));
                final int localColumnId = IdUtils.getLocalColumnId(globalId);
                Assert.assertFalse(localColumnId >= IdUtils.MIN_COLUMN_NUMBER
                        && localColumnId <= IdUtils.MAX_COLUMN_NUMBER);
            }
        }
    }

    @Test
    public void testSchemaIdsHaveNoValidTableAndColumn() {
        final List<Integer> schemaIds = Arrays.asList(IdUtils.MIN_SCHEMA_NUMBER, IdUtils.MIN_SCHEMA_NUMBER + 1,
                IdUtils.MAX_SCHEMA_NUMBER - 1, IdUtils.MAX_SCHEMA_NUMBER);
        for (final int schemaId : schemaIds) {
            final int globalId = IdUtils.createGlobalId(schemaId);

            if (VERBOSE) {
                System.out.format("[%s] (%3d, ----, ----) -> %9x\n", getClass().getSimpleName(), schemaId, globalId);
            }

            Assert.assertEquals(schemaId, IdUtils.getLocalSchemaId(globalId));
            final int localTableId = IdUtils.getLocalTableId(globalId);
            Assert.assertFalse(localTableId >= IdUtils.MIN_TABLE_NUMBER
                    && localTableId <= IdUtils.MAX_TABLE_NUMBER);
            final int localColumnId = IdUtils.getLocalColumnId(globalId);
            Assert.assertFalse(localColumnId >= IdUtils.MIN_COLUMN_NUMBER
                    && localColumnId <= IdUtils.MAX_COLUMN_NUMBER);
        }
    }

    @Test
    public void testGenerationOfIds() {
        final MetadataStore store = new DefaultMetadataStore();
        store.addSchema("foo", mock(Location.class)).addTable(store, "bar", mock(Location.class))
                .addColumn(store, "column1", 0);
        assertEquals(0b111111111111111111111111, store.getSchema("foo").getId());
        assertEquals(0b000000000000111111111111, store.getSchema("foo").getTable("bar").getId());
        assertEquals(0b000000000000000000000000, store.getSchema("foo").getTable("bar").getColumns().iterator().next()
                .getId());

        store.addSchema("foo2", mock(Location.class)).addTable(store, "bar2", mock(Location.class))
                .addColumn(store, "column1", 0);
        assertEquals(0b1111111111111111111111111, store.getSchema("foo2").getId());
        assertEquals(0b1000000000000111111111111, store.getSchema("foo2").getTable("bar2").getId());
        assertEquals(0b1000000000000000000000000, store.getSchema("foo2").getTable("bar2").getColumns().iterator()
                .next().getId());
    }

}

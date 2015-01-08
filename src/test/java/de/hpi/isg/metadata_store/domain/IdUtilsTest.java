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

    private IdUtils idUtils = new IdUtils(12, 12);

    @Test
    public void testIdAssembling() {
        final List<Integer> schemaIds = Arrays.asList(idUtils.getMinSchemaNumber(), idUtils.getMinSchemaNumber() + 1,
                idUtils.getMaxSchemaNumber() - 1, idUtils.getMaxSchemaNumber());
        final List<Integer> tableIds = Arrays.asList(idUtils.getMinTableNumber(), idUtils.getMinTableNumber() + 1,
                idUtils.getMaxTableNumber() - 1, idUtils.getMaxTableNumber());
        final List<Integer> columnIds = Arrays.asList(idUtils.getMinColumnNumber(), idUtils.getMinColumnNumber() + 1,
                idUtils.getMaxColumnNumber() - 1, idUtils.getMaxColumnNumber());
        for (final int schemaId : schemaIds) {
            for (final int tableId : tableIds) {
                for (final int columnId : columnIds) {
                    final int globalId = idUtils.createGlobalId(schemaId, tableId, columnId);

                    if (VERBOSE) {
                        System.out.format("[%s] (%3d, %4d, %4d) -> %9x\n", getClass().getSimpleName(), schemaId,
                                tableId, columnId, globalId);
                    }

                    Assert.assertEquals(schemaId, idUtils.getLocalSchemaId(globalId));
                    Assert.assertEquals(tableId, idUtils.getLocalTableId(globalId));
                    Assert.assertEquals(columnId, idUtils.getLocalColumnId(globalId));
                }
            }
        }
    }

    @Test
    public void testTableIdsHaveNoValidColumn() {
        final List<Integer> schemaIds = Arrays.asList(idUtils.getMinSchemaNumber(), idUtils.getMinSchemaNumber() + 1,
                idUtils.getMaxSchemaNumber() - 1, idUtils.getMaxSchemaNumber());
        final List<Integer> tableIds = Arrays.asList(idUtils.getMinTableNumber(), idUtils.getMinTableNumber() + 1,
                idUtils.getMaxTableNumber() - 1, idUtils.getMaxTableNumber());
        for (final int schemaId : schemaIds) {
            for (final int tableId : tableIds) {
                final int globalId = idUtils.createGlobalId(schemaId, tableId);

                if (VERBOSE) {
                    System.out.format("[%s] (%2d, %7d, ----) -> %9x\n", getClass().getSimpleName(), schemaId,
                            tableId, globalId);
                }

                Assert.assertEquals(schemaId, idUtils.getLocalSchemaId(globalId));
                Assert.assertEquals(tableId, idUtils.getLocalTableId(globalId));
                final int localColumnId = idUtils.getLocalColumnId(globalId);
                Assert.assertFalse(localColumnId >= idUtils.getMinColumnNumber()
                        && localColumnId <= idUtils.getMaxColumnNumber());
            }
        }
    }

    @Test
    public void testSchemaIdsHaveNoValidTableAndColumn() {
        final List<Integer> schemaIds = Arrays.asList(idUtils.getMinSchemaNumber(), idUtils.getMinSchemaNumber() + 1,
                idUtils.getMaxSchemaNumber() - 1, idUtils.getMaxSchemaNumber());
        for (final int schemaId : schemaIds) {
            final int globalId = idUtils.createGlobalId(schemaId);

            if (VERBOSE) {
                System.out.format("[%s] (%3d, ----, ----) -> %9x\n", getClass().getSimpleName(), schemaId, globalId);
            }

            Assert.assertEquals(schemaId, idUtils.getLocalSchemaId(globalId));
            final int localTableId = idUtils.getLocalTableId(globalId);
            Assert.assertFalse(localTableId >= idUtils.getMinTableNumber()
                    && localTableId <= idUtils.getMaxTableNumber());
            final int localColumnId = idUtils.getLocalColumnId(globalId);
            Assert.assertFalse(localColumnId >= idUtils.getMinColumnNumber()
                    && localColumnId <= idUtils.getMaxColumnNumber());
        }
    }

    @Test
    public void testGenerationOfIds() {
        final MetadataStore store = new DefaultMetadataStore();
        store.addSchema("foo", null, mock(Location.class)).addTable(store, "bar", null, mock(Location.class))
                .addColumn(store, "column1", null, 0);
        assertEquals(0b111111111111111111111111, store.getSchemaByName("foo").getId());
        assertEquals(0b000000000000111111111111, store.getSchemaByName("foo").getTableByName("bar").getId());
        assertEquals(0b000000000000000000000000, store.getSchemaByName("foo").getTableByName("bar").getColumns()
                .iterator().next()
                .getId());

        store.addSchema("foo2", null, mock(Location.class)).addTable(store, "bar2", null, mock(Location.class))
                .addColumn(store, "column1", null, 0);
        assertEquals(0b1111111111111111111111111, store.getSchemaByName("foo2").getId());
        assertEquals(0b1000000000000111111111111, store.getSchemaByName("foo2").getTableByName("bar2").getId());
        assertEquals(0b1000000000000000000000000, store.getSchemaByName("foo2").getTableByName("bar2").getColumns()
                .iterator()
                .next().getId());
    }

    @Test
    public void testIdTypeTest() {
        Assert.assertTrue(idUtils.isSchemaId(0b111111111111111111111111));
        Assert.assertFalse(idUtils.isTableId(0b1111111111111111111111111));

        Assert.assertFalse(idUtils.isSchemaId(0b000000000000111111111111));
        Assert.assertTrue(idUtils.isTableId(0b1000000000000111111111111));

        Assert.assertFalse(idUtils.isSchemaId(0b000000000000000000000000));
        Assert.assertFalse(idUtils.isTableId(0b1000000000000000000000000));
    }
}

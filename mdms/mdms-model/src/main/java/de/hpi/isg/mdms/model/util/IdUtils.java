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
package de.hpi.isg.mdms.model.util;

import java.io.Serializable;

import de.hpi.isg.mdms.model.MetadataStore;

/**
 * This class offers some utility functionality to deal with IDs in a {@link MetadataStore}. Thereby, it obeys the
 * following conventions:
 * <ol>
 * <li>The 32 bit of each ID are assigned to different elements: <tt>xxx... yyy... zzz....</tt></li>
 * <li>the high {@value #numSchemaBits} bits are used to encode the schema (<tt>x</tt>)</li>
 * <li>the mid {@value #numTableBits} bits are used to encode the table (<tt>y</tt>)</li>
 * <li>the low {@value #numColumnBits} bits are used to encode the column (<tt>y</tt>)</li>
 * <li>for schema IDs and table IDs the column (and the table bits) are all set to 1</li>
 * </ol>
 */
@SuppressWarnings("serial")
public class IdUtils implements Serializable {

    public enum IdTypes {SCHEMA_ID, TABLE_ID, COLUMN_ID}

    public static final int DEFAULT_NUM_COLUMN_BITS = 12;
    public static final int DEFAULT_NUM_TABLE_BITS = 12;

    private final int numTableBits;
    private final int numColumnBits;
    private final int numSchemaBits;

    private final int schemaIdBitmask;
    private final int schemaIdOffset;

    private final int tableIdBitmask;
    private final int tableIdOffset;

    private final int columnIdBitmask;

    public final int minSchemaNumber;
    public final int maxSchemaNumber; // 2 ^ 4

    public final int minTableNumber;
    public final int maxTableNumber; // 2^20 - 2

    public final int minColumnNumber;
    public final int maxColumnNumber; // 2^8 - 2

    private static int toBitMask(int numBits) {
        return twoToThePowerOf(numBits) - 1;
    }

    private static int twoToThePowerOf(int x) {
        return 1 << x;
    }

    public IdUtils(int numTableBits, int numColumnBits) {
        this.numSchemaBits = 32 - numTableBits - numColumnBits;
        this.numTableBits = numTableBits;
        this.numColumnBits = numColumnBits;

        schemaIdBitmask = toBitMask(numSchemaBits);
        schemaIdOffset = numTableBits + numColumnBits;

        tableIdBitmask = toBitMask(numTableBits);
        tableIdOffset = numColumnBits;

        columnIdBitmask = toBitMask(numColumnBits);

        minSchemaNumber = 0;
        maxSchemaNumber = twoToThePowerOf(numSchemaBits) - 1; // 2 ^ 4

        minTableNumber = 0;
        maxTableNumber = twoToThePowerOf(numTableBits) - 2; // 2^20 - 2

        minColumnNumber = 0;
        maxColumnNumber = twoToThePowerOf(numColumnBits) - 2; // 2^8 - 2
    }

    /**
     * Creates a global ID for the specified target.
     *
     * @param localSchemaId is a unique number for the schema between {@value #minSchemaNumber} and {@value #maxSchemaNumber}
     * @param localTableId  is a unique number for the table within its schema between {@value #minTableNumber} and
     *                      {@value #maxTableNumber}
     * @param localColumnId is the offset of the column within its table between {@value #minColumnNumber} and
     *                      {@value #maxColumnNumber}
     * @return the global ID
     */
    public int createGlobalId(final int localSchemaId, final int localTableId, final int localColumnId) {
        return ((localSchemaId & schemaIdBitmask) << schemaIdOffset)
                | ((localTableId & tableIdBitmask) << tableIdOffset)
                | ((localColumnId) & columnIdBitmask);
    }

    /**
     * Creates a global ID for the specified target.
     *
     * @param localSchemaId is a unique number for the schema between {@value #minSchemaNumber} and {@value #maxSchemaNumber}
     * @param localTableId  is a unique number for the table within its schema between {@value #minTableNumber} and
     *                      {@value #maxTableNumber}
     * @return the global ID
     */
    public int createGlobalId(final int localSchemaId, final int localTableId) {
        return createGlobalId(localSchemaId, localTableId, maxColumnNumber + 1);
    }

    /**
     * Creates a global ID for the specified target.
     *
     * @param localSchemaId is a unique number for the schema between {@value #minSchemaNumber} and {@value #maxSchemaNumber}
     * @return the global ID
     */
    public int createGlobalId(final int localSchemaId) {
        return createGlobalId(localSchemaId, maxTableNumber + 1, maxColumnNumber + 1);
    }

    /**
     * Returns the local schema ID that is encoded in the given global ID.
     *
     * @param globalId is the ID from which the local schema ID shall be extracted
     * @return the local schema ID
     */
    public int getLocalSchemaId(final int globalId) {
        return (globalId >> schemaIdOffset) & schemaIdBitmask;
    }

    /**
     * Returns the local table ID that is encoded in the given global ID.
     *
     * @param globalId is the ID from which the local table ID shall be extracted
     * @return the local table ID
     */
    public int getLocalTableId(final int globalId) {
        return (globalId >> tableIdOffset) & tableIdBitmask;
    }

    /**
     * Returns the local column ID that is encoded in the given global ID.
     *
     * @param globalId is the ID from which the local column ID shall be extracted
     * @return the local column ID
     */
    public int getLocalColumnId(final int globalId) {
        return globalId & columnIdBitmask;
    }

    public boolean isSchemaId(final int id) {
        return (getLocalTableId(id) > maxTableNumber && getLocalColumnId(id) > maxColumnNumber);
    }

    public boolean isTableId(final int id) {
        return (getLocalColumnId(id) > maxColumnNumber && !(getLocalTableId(id) > maxTableNumber));
    }

    public int getNumTableBits() {
        return numTableBits;
    }

    public int getNumColumnBits() {
        return numColumnBits;
    }

    public int getNumSchemaBits() {
        return numSchemaBits;
    }

    public int getMinSchemaNumber() {
        return minSchemaNumber;
    }

    public int getMaxSchemaNumber() {
        return maxSchemaNumber;
    }

    public int getMinTableNumber() {
        return minTableNumber;
    }

    public int getMaxTableNumber() {
        return maxTableNumber;
    }

    public int getMinColumnNumber() {
        return minColumnNumber;
    }

    public int getMaxColumnNumber() {
        return maxColumnNumber;
    }

    public int getTableId(int id) {
        int schemaNumber = getLocalSchemaId(id);
        int tableNumber = getLocalTableId(id);
        if (tableNumber == this.maxTableNumber + 1) {
            throw new IllegalArgumentException("Cannot create a table ID for the ID " + id);
        }
        return createGlobalId(schemaNumber, tableNumber);
    }

    public int getSchemaId(int id) {
        int schemaNumber = getLocalSchemaId(id);
        return createGlobalId(schemaNumber);
    }

    public IdTypes getIdType(int id) {
        if (isSchemaId(id)) {
            return IdTypes.SCHEMA_ID;
        } else if (isTableId(id)) {
            return IdTypes.TABLE_ID;
        }
        return IdTypes.COLUMN_ID;
    }

    @Override
    public String toString() {
        return String.format("IdUtils [%d/%d/%d]", this.numSchemaBits, this.numColumnBits, this.numTableBits);
    }

    // Target1 contained in Target2
    public boolean isContained(int id1, int id2) {
        if (isSchemaId(id2)) {
            if (getSchemaId(id1) == getSchemaId(id2)) {
                return true;
            }
        } else if (isTableId(id2)) {
            if (!isSchemaId(id1) && getSchemaId(id1) == getSchemaId(id2) && getTableId(id1) == getTableId(id2)) {
                return true;
            }
        } else {
            if (!isTableId(id1) && !isSchemaId(id1)) {
                if (id1 == id2) {
                    return true;
                }
            }
        }
        return false;
    }
}

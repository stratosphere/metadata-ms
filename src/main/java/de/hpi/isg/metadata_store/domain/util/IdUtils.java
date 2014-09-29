/***********************************************************************************************************************
 * Copyright (C) 2014 by Sebastian Kruse
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/
package de.hpi.isg.metadata_store.domain.util;

import de.hpi.isg.metadata_store.domain.MetadataStore;

/**
 * This class offers some utility functionality to deal with IDs in a {@link MetadataStore}. Thereby, it obeys the
 * following conventions:
 * <ol>
 * <li>The 32 bit of each ID are assigned to different elements: <tt>xxxxxxxx yyyyyyyy yyyyzzzz zzzzzzzz</tt></li>
 * <li>the high 8 bits are used to encode the schema (<tt>x</tt>, 256 combinations)</li>
 * <li>the mid 12 bits are used to encode the table (<tt>y</tt>, 4096 combinations per schema)</li>
 * <li>the low 12 bits are used to encode the column (<tt>y</tt>, 4096 combinations per table)</li>
 * <li>schema IDs are of the form <tt>xxxxxxxx 11111111 11111111 11111111</tt></li>
 * <li>table IDs are of the form <tt>xxxxxxxx yyyyyyyy yyyy1111 11111111</tt></li>
 * </ol>
 */
public class IdUtils {

	public static final int MIN_SCHEMA_NUMBER = 0;
	public static final int MAX_SCHEMA_NUMBER = 255;

	public static final int MIN_TABLE_NUMBER = 0;
	public static final int MAX_TABLE_NUMBER = 4094; // 2^12 - 2

	public static final int MIN_COLUMN_NUMBER = 0;
	public static final int MAX_COLUMN_NUMBER = 4094; // 2^12 - 2

	private IdUtils() {}

	/**
	 * Creates a global ID for the specified target.
	 * 
	 * @param localSchemaId
	 *            is a unique number for the schema between {@value #MIN_SCHEMA_NUMBER} and {@value #MAX_SCHEMA_NUMBER}
	 * @param localTableId
	 *            is a unique number for the table within its schema between {@value #MIN_TABLE_NUMBER} and
	 *            {@value #MAX_TABLE_NUMBER}
	 * @param localColumnId
	 *            is the offset of the column within its table between {@value #MIN_COLUMN_NUMBER} and {@value #MAX_COLUMN_NUMBER}
	 * @return the global ID
	 */
	public static int createGlobalId(int localSchemaId, int localTableId, int localColumnId) {
		return ((localSchemaId & 0xFF) << 24) | ((localTableId & 0xFFF) << 12) | ((localColumnId) & 0xFFF);
	}

	/**
	 * Creates a global ID for the specified target.
	 * 
	 * @param localSchemaId
	 *            is a unique number for the schema between {@value #MIN_SCHEMA_NUMBER} and {@value #MAX_SCHEMA_NUMBER}
	 * @param localTableId
	 *            is a unique number for the table within its schema between {@value #MIN_TABLE_NUMBER} and
	 *            {@value #MAX_TABLE_NUMBER}
	 * @return the global ID
	 */
	public static int createGlobalId(int localSchemaId, int localTableId) {
		return createGlobalId(localSchemaId, localTableId, MAX_COLUMN_NUMBER + 1);
	}

	/**
	 * Creates a global ID for the specified target.
	 * 
	 * @param localSchemaId
	 *            is a unique number for the schema between {@value #MIN_SCHEMA_NUMBER} and {@value #MAX_SCHEMA_NUMBER}
	 * @return the global ID
	 */
	public static int createGlobalId(int localSchemaId) {
		return createGlobalId(localSchemaId, MAX_TABLE_NUMBER + 1, MAX_COLUMN_NUMBER + 1);
	}

	/**
	 * Returns the local schema ID that is encoded in the given global ID.
	 * 
	 * @param globalId
	 *            is the ID from which the local schema ID shall be extracted
	 * @return the local schema ID
	 */
	public static int getLocalSchemaId(int globalId) {
		return (globalId >> 24) & 0xFF;
	}

	/**
	 * Returns the local table ID that is encoded in the given global ID.
	 * 
	 * @param globalId
	 *            is the ID from which the local table ID shall be extracted
	 * @return the local table ID
	 */
	public static int getLocalTableId(int globalId) {
		return (globalId >> 12) & 0xFFF;
	}

	/**
	 * Returns the local column ID that is encoded in the given global ID.
	 * 
	 * @param globalId
	 *            is the ID from which the local column ID shall be extracted
	 * @return the local column ID
	 */
	public static int getLocalColumnId(int globalId) {
		return globalId & 0xFFF;
	}

}

/***********************************************************************************************************************
 * Copyright (C) 2014 by Sebastian Kruse
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/
package de.hpi.isg.mdms.clients.location;

import de.hpi.isg.mdms.model.location.DefaultLocation;
import de.hpi.isg.mdms.model.targets.Table;

/**
 * Describes a partition of a merged CSV file. This partition identifies a {@link Table}.
 *
 * @author Sebastian Kruse
 * @see MergedCsvFileLocation
 */
@SuppressWarnings("serial")
public class MergedCsvFilePartitionLocation extends DefaultLocation {

    public static final String PARTITION_ID = "PARTITION";

    public int getPartitionId() {
        return Integer.parseInt(get(PARTITION_ID));
    }

    public void setPartitionId(int partitionId) {
        set(PARTITION_ID, String.valueOf(partitionId));
    }

}

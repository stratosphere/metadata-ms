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
package de.hpi.isg.mdms.flink.location;

import de.hpi.isg.mdms.model.location.Location;
import de.hpi.isg.mdms.model.targets.Table;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * This interface describes locations of tables that can be read out as cells, i.e., as tuples of column IDs and the
 * tuple value.
 * 
 * @author Sebastian Kruse
 */
public interface CellLocation extends Location {

    /**
	 * @return a utility object that creates a {@link DataSet} for this location.
	 */
	<TLocation extends CellLocation> DataSourceBuilder<Table, TLocation, Tuple2<Integer, String>> getCellDataSourceBuilder();

}

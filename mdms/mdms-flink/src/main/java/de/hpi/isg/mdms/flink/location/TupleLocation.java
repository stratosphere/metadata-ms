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

import de.hpi.isg.mdms.flink.data.Tuple;
import de.hpi.isg.mdms.model.location.Location;
import de.hpi.isg.mdms.model.targets.Table;

/**
 * This interface describes locations of tables that can be read out as tuples of table IDs and a raw tuple string. 
 *
 * @author Sebastian Kruse
 */
public interface TupleLocation extends Location {
    
    <TLocation extends TupleLocation> DataSourceBuilder<Table, TLocation, Tuple> getTupleDataSourceBuilder();

}

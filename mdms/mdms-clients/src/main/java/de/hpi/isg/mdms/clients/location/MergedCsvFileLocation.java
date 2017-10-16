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
package de.hpi.isg.mdms.clients.location;

/**
 * Describes the location of a merged CSV file, which is a CSV file where
 * <ul>
 * <li>the first field in each row is the ID of a table to which this row belongs and</li>
 * <li>the remainder fields form the tuple.</li>
 * </ul>
 * As such, a {@link MergedCsvFileLocation} can comprise multiple {@link de.hpi.isg.mdms.model.targets.Table}s and
 * perhaps a whole {@link de.hpi.isg.mdms.model.targets.Schema}.
 *
 * @author Sebastian Kruse
 */
@SuppressWarnings("serial")
public class MergedCsvFileLocation extends AbstractCsvLocation {

}

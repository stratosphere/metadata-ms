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
package de.hpi.isg.mdms.flink.data;

import de.hpi.isg.mdms.model.targets.Column;

import java.io.Serializable;

/**
 * A cell consists of an {@link Column} ID and a value coming from that column.
 *
 * @author Sebastian Kruse
 */
@SuppressWarnings("serial")
public class Cell implements Serializable {
    
    private int columnId;
    
    private String value;
    
    public Cell() {
        super();
    }

    public Cell(int columnId, String value) {
        super();
        this.columnId = columnId;
        this.value = value;
    }

    public int getColumnId() {
        return columnId;
    }

    public void setColumnId(int columnId) {
        this.columnId = columnId;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "Cell [columnId=" + columnId + ", value=" + value + "]";
    }
    
}

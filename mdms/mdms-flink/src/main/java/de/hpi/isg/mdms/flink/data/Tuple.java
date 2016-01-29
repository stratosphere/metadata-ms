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

import de.hpi.isg.mdms.model.targets.Table;

import java.io.Serializable;
import java.util.Arrays;

/**
 * A tuple consists of an {@link Table} ID and tuple values from that table
 *
 * @author Sebastian Kruse
 */
@SuppressWarnings("serial")
public class Tuple implements Serializable {
    
    private int minColumnId;
    
    private String[] fields;
    
    public Tuple() {
        super();
    }

    public Tuple(int minColumnId, String[] fields) {
        super();
        this.minColumnId = minColumnId;
        this.fields = fields;
    }

    /**
     * Checks whether any of the {@link #fields} is {@code null} or an empty {@link String}.
     * @return the test result
     */
    public boolean hasEmptyOrNullField() {
        for (String field : this.fields) {
            if (field == null || field.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    public int getMinColumnId() {
        return minColumnId;
    }

    public void setMinColumnId(int minColumnId) {
        this.minColumnId = minColumnId;
    }

    public String[] getFields() {
        return fields;
    }

    public void setFields(String[] fields) {
        this.fields = fields;
    }

    @Override
    public String toString() {
        return "Tuple [minColumnId=" + minColumnId + ", fields=" + Arrays.toString(fields) + "]";
    }

}

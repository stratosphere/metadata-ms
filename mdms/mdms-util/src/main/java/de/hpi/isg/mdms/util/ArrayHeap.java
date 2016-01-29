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
package de.hpi.isg.mdms.util;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

/**
 * This heap creates arrays on demand and reuses them if possible.
 * 
 * @author Sebastian Kruse
 */
@SuppressWarnings("serial")
public class ArrayHeap<T> implements Serializable {

    private final List<T[]> cache = new ArrayList<>();
    
    private final Class<T> memberClass;
    
    public ArrayHeap(Class<T> memberClass) {
        this.memberClass = memberClass;
    }

    @SuppressWarnings("unchecked")
    public T[] yield(final int length) {
        while (this.cache.size() < length + 1) {
            this.cache.add(null);
        }

        T[] array = this.cache.get(length);
        if (array == null) {
            array = (T[]) Array.newInstance(this.memberClass, length);
            this.cache.set(length, array);
        }

        return array;
    }

}

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

import java.util.ArrayList;
import java.util.List;

/**
 * This heap creates byte[] arrays on demand and reuses them if possible.
 * 
 * @author Sebastian Kruse
 */
public class ByteArrayHeap {

    private final List<byte[]> cache = new ArrayList<>();

    public byte[] yield(final int length) {
        while (this.cache.size() < length + 1) {
            this.cache.add(null);
        }

        byte[] bytes = this.cache.get(length);
        if (bytes == null) {
            bytes = new byte[length];
            this.cache.set(length, bytes);
        }

        return bytes;
    }

}

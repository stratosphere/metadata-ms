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

import de.hpi.isg.mdms.model.location.DefaultLocation;

/**
 * This {@link de.hpi.isg.mdms.model.location.Location} type is associated with a file path.
 *
 * @author Sebastian Kruse
 */
@SuppressWarnings("serial")
public class FileLocation extends DefaultLocation {

    public void setPath(String path) {
        this.set(PATH, path);
    }

    public String getPath() {
        return this.get(PATH);
    }

}

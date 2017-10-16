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


import de.hpi.isg.mdms.Encoding;
import de.hpi.isg.mdms.clients.parameters.CsvParameters;
import de.hpi.isg.mdms.model.targets.Table;

import java.util.*;

/**
 * Base class for locations that contain CSV-formatted content.
 *
 * @author Sebastian Kruse
 */
@SuppressWarnings("serial")
public abstract class AbstractCsvLocation extends FileLocation {

    public static final String SEPARATOR = "SEPARATOR";

    public static final String QUOTE_CHAR = "QUOTE";

    public static final String HAS_HEADER = "HEADER";

    public static final String ENCODING = "ENC";

    public static final String NULL_STRING = "NULLSTR";

    public static final char NO_QUOTE_CHAR = CsvParameters.NO_QUOTE_CHAR;

    public char getQuoteChar() {
        String quoteChar = getIfExists(QUOTE_CHAR);
        if (quoteChar == null || quoteChar.isEmpty()) {
            return NO_QUOTE_CHAR;
        }
        return quoteChar.charAt(0);
    }

    public char getFieldSeparator() {
        return get(SEPARATOR).charAt(0);
    }

    public boolean getHasHeader() {
        String hasHeader = getIfExists(HAS_HEADER);
        if (hasHeader == null || hasHeader.isEmpty()) {
            return false;
        }
        if (hasHeader.equalsIgnoreCase("true")) {
            return true;
        } else {
            return false;
        }
    }

    public void setFieldSeparator(char fieldSeparator) {
        set(SEPARATOR, String.valueOf(fieldSeparator));
    }

    public void setQuoteChar(char quoteChar) {
        if (quoteChar == NO_QUOTE_CHAR) {
            delete(QUOTE_CHAR);
        } else {
            set(QUOTE_CHAR, String.valueOf(quoteChar));
        }
    }

    public void setHasHeader(boolean hasHeader) {
        set(HAS_HEADER, Boolean.toString(hasHeader));
    }

    public void setEncoding(Encoding encoding) {
        if (encoding != null && !(encoding.equals(Encoding.DEFAULT_ENCODING))) {
            set(ENCODING, encoding.toConfigString());
        } else {
            delete(ENCODING);
        }
    }

    public Encoding getEncoding() {
        String configString = getIfExists(ENCODING);
        if (configString != null && !configString.isEmpty()) {
            return Encoding.fromConfigString(configString);
        } else {
            return Encoding.DEFAULT_ENCODING;
        }
    }

    public void setNullString(String nullString) {
        if (nullString == null) {
            this.delete(NULL_STRING);
        } else {
            this.set(NULL_STRING, nullString);
        }
    }

    public String getNullString() {
        return getIfExists(NULL_STRING);
    }

    public CsvParameters getCsvParameters() {
        return new CsvParameters(getFieldSeparator(), getQuoteChar(), getNullString());
    }

    @Override
    public Collection<String> getAllPropertyKeys() {
        LinkedList<String> allPropertyKeys = new LinkedList<>(super.getAllPropertyKeys());
        allPropertyKeys.add(QUOTE_CHAR);
        allPropertyKeys.add(SEPARATOR);
        allPropertyKeys.add(ENCODING);
        allPropertyKeys.add(HAS_HEADER);
        allPropertyKeys.add(NULL_STRING);
        return allPropertyKeys;
    }

    @Override
    public Collection<String> getPropertyKeysForValueCanonicalization() {
        LinkedList<String> propertyKeys = new LinkedList<>(super.getPropertyKeysForValueCanonicalization());
        propertyKeys.add(QUOTE_CHAR);
        propertyKeys.add(SEPARATOR);
        propertyKeys.add(ENCODING);
        propertyKeys.add(HAS_HEADER);
        propertyKeys.add(NULL_STRING);
        return propertyKeys;
    }

    /**
     * Creates a mapping from paths to non-standard encodings.
     *
     * @param tables are the tables whose encodings are to collect
     * @return a mapping pointing the tables' paths to their encoding (if it is not the default encoding)
     */
    public static Map<String, Encoding> collectEncodings(List<Table> tables) {
        Map<String, Encoding> encodings = new HashMap<>();
        for (Table table : tables) {
            AbstractCsvLocation location = (AbstractCsvLocation) table.getLocation();
            Encoding encoding = location.getEncoding();
            if (encoding != null && !encoding.equals(Encoding.DEFAULT_ENCODING)) {
                encodings.put(location.getPath(), encoding);
            }
        }
        return encodings;
    }
}
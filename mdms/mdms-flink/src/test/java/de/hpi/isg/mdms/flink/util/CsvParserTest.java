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
package de.hpi.isg.mdms.flink.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests for {@link CsvParser}.
 * 
 * @author Sebastian Kruse
 */
public class CsvParserTest {

    @Test
    public void testSimpleParsing() throws Exception {
        String line;

        final CsvParser parser = new CsvParser(',', '\"', null);

        line = "\"a\","
                + "\"bba\","
                + ","
                + "\"a,b,c\"";
        final List<String> expectation = Arrays.asList("a", "bba", null, "a,b,c");

        final List<String> result = parser.parse(line);

        Assert.assertEquals(expectation, result);
    }

    @Test
    public void testParsingMoreThanOnce() throws Exception {
        String line;
        List<String> expectation;
        List<String> result;

        final CsvParser parser = new CsvParser(',', '\0', null);

        line = "1,2,3,4";
        expectation = Arrays.asList("1,2,3,4".split(","));
        result = parser.parse(line);
        Assert.assertEquals(expectation, result);

        line = "1,2,3,4,5";
        expectation = Arrays.asList("1,2,3,4,5".split(","));
        result = parser.parse(line);
        Assert.assertEquals(expectation, result);

        line = "1,2";
        expectation = Arrays.asList("1,2".split(","));
        result = parser.parse(line);
        Assert.assertEquals(expectation, result);
    }
    
    @Test
    public void splittingEasyLinesShouldWork() throws Exception {

        CsvParser parser = new CsvParser(';', '"', null);

        // "Hello";;"World;";"";!;
        String testRow = "\"Hello\";;\"World;\";\"\";!;\"\"";
        List<String> expectedResult = Arrays.asList("Hello", null, "World;", null, "!", null);
        List<String> result = parser.parse(testRow); 
        
        Assert.assertEquals(expectedResult, result);
    }
    
    // TODO: @Test
    public void pendingSeparatorsShouldSpawnField() throws Exception {
        
        CsvParser parser = new CsvParser(';', '"', null);
        
        // "Hello";;"World;";"";!;
        String testRow = "\"Hello\";;\"World;\";\"\";!;";
        List<String> expectedResult = Arrays.asList("Hello", null, "World;", null, "!", null);
        List<String> result = parser.parse(testRow); 
        
        Assert.assertEquals(expectedResult, result);
    }
    
    @Test
    public void twoConsequentQuotesShouldBeEscaped() throws Exception {
        CsvParser parser = new CsvParser(';', '"', null);
        
        // "Hello ""World""";""";""";"""World""";"""";"""""";"";""
        String testRow = "\"Hello \"\"World\"\"\";"
                + "\"\"\";\"\"\";"
                + "\"\"\"World\"\"\";"
                + "\"\"\"\";"
                + "\"\"\"\"\"\";"
                + "\"\";"
                + "\"\"";
        List<String> expectedResult = Arrays.asList("Hello \"World\"", "\";\"", "\"World\"", "\"", "\"\"", null, null);
        List<String> result = parser.parse(testRow); 
        
        Assert.assertEquals(expectedResult, result);
    }

}

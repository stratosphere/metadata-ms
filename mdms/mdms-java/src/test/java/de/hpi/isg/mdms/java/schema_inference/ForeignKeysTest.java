package de.hpi.isg.mdms.java.schema_inference;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test suite for {@link ForeignKeys}.
 */
public class ForeignKeysTest {

    @Test
    public void testLongestSubstringLength() {
        Assert.assertEquals(0, ForeignKeys.longestSubstringLength("", "abc"));
        Assert.assertEquals(0, ForeignKeys.longestSubstringLength("abc", ""));
        Assert.assertEquals(3, ForeignKeys.longestSubstringLength("abc", "abcd"));
        Assert.assertEquals(3, ForeignKeys.longestSubstringLength("abcd", "abc"));
        Assert.assertEquals(6, ForeignKeys.longestSubstringLength("abababbad", "ababba"));
        Assert.assertEquals(5, ForeignKeys.longestSubstringLength("cababababac", "dababad"));
    }

}
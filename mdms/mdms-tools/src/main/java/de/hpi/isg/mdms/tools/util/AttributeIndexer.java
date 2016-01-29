package de.hpi.isg.mdms.tools.util;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.Serializable;
import java.util.*;

/**
 * An {@link AttributeIndexer} helps to assign IDs to a set of attributes from different containers (e.g. columns from
 * different tables). Furthermore, it allows to reverse-lookup the attribute name for an attribute ID.
 * 
 * @author Sebastian Kruse
 */
public class AttributeIndexer implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Contains ranges of valid attribute IDs.
     */
    private final IntArrayList ranges = new IntArrayList();

    /**
     * Mirror of the <tt>ranges</tt> field.
     */
    private int[] rangesArray;

    /**
     * Contains the attribute container names. The index of a container is its container ID.
     */
    private final List<String> sourceNames = new ArrayList<String>();

    /**
     * Holds the next valid attribute ID for a to-be-indexed attribute container.
     */
    private int nextOffset = 0;

    /**
     * Creates a new {@link AttributeIndexer} from a set of tuples that should have been created formerly with
     * {@link #toTuples()}.
     * 
     * @param tuples
     *        is the set of tuples that discribe the {@link AttributeIndexer} state
     * @return the new {@link AttributeIndexer}
     */
    public static AttributeIndexer fromTuples(
            final Collection<Tuple3<String, Integer, Integer>> tuples) {

        // Sort the tuples according to their ID ranges.
        final List<Tuple3<String, Integer, Integer>> tupleList = new ArrayList<Tuple3<String, Integer, Integer>>(
                tuples);
        Collections.sort(tupleList,
                new Comparator<Tuple3<String, Integer, Integer>>() {

                    @Override
                    public int compare(
                            final Tuple3<String, Integer, Integer> tuple1,
                            final Tuple3<String, Integer, Integer> tuple2) {

                        return tuple1.f1 - tuple2.f1;
                    }
                });

        // Add all the tuple contents to a new instance.
        final AttributeIndexer attributeIndexer = new AttributeIndexer();
        for (final Tuple3<String, Integer, Integer> tuple : tuples) {
            attributeIndexer.addToIndex(tuple.f0, tuple.f1, tuple.f2);
        }
        return attributeIndexer;
    }

    /**
     * Returns a {@link String} representation of a given attribute:<br>
     * <i>&lt;container&gt;[&lt;attribute position&gt;]</i>
     * 
     * @param sourceName
     *        is the name of the container
     * @param attributePosition
     *        is the attributes position within the container
     * @return the attribute's {@link String} representation
     */
    public static String formatAttributeName(final String sourceName,
            final int attributePosition) {

        return sourceName + "[" + attributePosition + "]";
    }

    /**
     * Pads a given value.
     * 
     * @param offset
     *        is the value to be padded
     * @return the padded value
     */
    private static int pad(final int offset) {
        return ((offset / 1000) + 1) * 1000;
    }

    /**
     * Calculates the ID for an attribute based on its source offset (as provided by {@link #index(String, int)} and
     * {@link #getContainerOffset(String)}) and the position of the attribute within its container.
     * 
     * @param sourceOffset
     *        is the offset of the attribute container
     * @param attributePosition
     *        is the position of the attribute within its attribute container
     * @return the ID of the attribute
     */
    public static int calculateAttributeId(final int sourceOffset,
            final int attributePosition) {
        return sourceOffset + attributePosition;
    }

    /**
     * Add attributes from a container to the index.
     * 
     * @param sourceName
     *        is the name of the attribute container
     * @param numAttributes
     *        is the number of attributes found in the container
     * @return the base offset for the indexed attribute container or -1 if no such offset has been allocated
     */
    public int index(final String sourceName, final int numAttributes) {

        // Calculate lower and upper limit for the new attribute ID range and a
        // possible new offset for the next attribute container.
        final int startIndex = this.nextOffset;
        final int endIndex = startIndex + numAttributes;

        addToIndex(sourceName, startIndex, endIndex);

        // Return the beginning of the attribute ID range for the indexed
        // container.
        return numAttributes > 0 ? startIndex : -1;
    }

    /**
     * Adds a new attribute container to the internal data structures of an {@link AttributeIndexer}.
     * 
     * @param sourceName
     *        is the name of the attribute container
     * @param startIndex
     *        is the first index of the container's attribute ID range
     * @param endIndex
     *        is the first index after the container's attribute ID range
     */
    private void addToIndex(final String sourceName, final int startIndex,
            final int endIndex) {

        // We may only add the container if it actually contains attributes.
        if (startIndex < endIndex) {

            // Add the new container.
            this.sourceNames.add(sourceName);

            // Add the new ID range for the new container.
            this.ranges.add(startIndex);
            this.ranges.add(endIndex);

            // rangesArray will be out of sync and must be recalculated as
            // needed.
            this.rangesArray = null;
        }

        // Pad between ID ranges to detect potential errors.
        this.nextOffset = pad(endIndex + 1);
    }

    /**
     * Given an ID of an indexed attribute, this method builds a {@link String} representation for this attribute.
     * 
     * @param attributeId
     *        is the ID of the attribute to be indexed
     * @return the attribute's {@link String} representation
     * @see #formatAttributeName(String, int)
     */
    public String resolve(final int attributeId) {

        // Lazy-initialize rangesArray.
        if (this.rangesArray == null) {
            this.rangesArray = this.ranges.toIntArray();
        }

        // Search in the range index the position of the given column.
        final int binarySearch = Arrays.binarySearch(this.rangesArray,
                attributeId);
        final int rangeIndex = binarySearch < 0 ? -(binarySearch + 2)
                : binarySearch;

        // This range index must be even: Odd values point to a "padding" range.
        if ((rangeIndex & 0x01) != 0) {
            throw new IllegalArgumentException(String.format(
                    "There is no column indexed with the id of %d.",
                    attributeId));
        }

        // Accordingly, the range index divided by 2 is the source ID.
        final int sourceId = rangeIndex >> 1;
        final String sourceName = this.sourceNames.get(sourceId);

        // Calculate the attribute index within its container.
        final int sourceOffset = this.rangesArray[rangeIndex];
        final int attributePosition = attributeId - sourceOffset;

        return formatAttributeName(sourceName, attributePosition);
    }

    /**
     * Returns the number of attributes for a given container.
     * 
     * @param sourceName
     *        is the name of the attribute container
     * @return the number of attributes for the given container or 0 if there is no such container indexed
     */
    public int getNumAttributes(final String sourceName) {
        final int containerId = this.sourceNames.indexOf(sourceName);
        if (containerId < 0) {
            return 0;
        } else {
            final int startIndex = this.ranges.getInt(containerId << 1);
            final int endIndex = this.ranges.getInt((containerId << 1) + 1);
            return endIndex - startIndex;
        }
    }

    /**
     * Returns the number of all indexed attributes.
     * 
     * @return the number of attributes
     */
    public int getNumAttributes() {
        int sum = 0;
        for (final String sourceName : this.sourceNames) {
            sum += getNumAttributes(sourceName);
        }
        return sum;
    }

    /**
     * Retrieves the container offset for a given container as it was also provided by {@link #index(String, int)}.
     * 
     * @param sourceName
     *        is the name of the container
     * @return the container offset or -1 if no container offset could be found
     */
    public int getContainerOffset(final String sourceName) {
        final int containerId = this.sourceNames.indexOf(sourceName);
        if (containerId < 0) {
            return -1;
        } else {
            return this.ranges.getInt(containerId << 1);
        }

    }

    /**
     * Converts the internal state of the {@link AttributeIndexer} as tuples of the following form:<br>
     * <i>(attribute container, min attribute ID, max attribute ID + 1)</i>
     * 
     * @return the internal state as tuples
     * @see #fromTuples(Collection)
     */
    public Collection<Tuple3<String, Integer, Integer>> toTuples() {

        final Collection<Tuple3<String, Integer, Integer>> tuples = new ArrayList<Tuple3<String, Integer, Integer>>();

        for (int sourceId = 0; sourceId < this.sourceNames.size(); sourceId++) {
            final Tuple3<String, Integer, Integer> tuple = new Tuple3<String, Integer, Integer>();
            tuple.f0 = this.sourceNames.get(sourceId);
            tuple.f1 = this.ranges.get(sourceId * 2);
            tuple.f2 = this.ranges.get(sourceId * 2 + 1);
            tuples.add(tuple);
        }

        return tuples;
    }

}

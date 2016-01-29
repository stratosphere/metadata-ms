package de.hpi.isg.mdms.util;

import java.lang.reflect.Array;
import java.util.*;

/**
 * This class gathers a set of static methods that support activities related to {@link Collection} handling.
 * 
 * @author Sebastian Kruse
 */
public class CollectionUtils {

    public static interface CollectionFactory<T extends Collection<?>> {

        T create();
    }

    private static final CollectionFactory<List<Object>> LIST_FACTORY = new CollectionFactory<List<Object>>() {

        @Override
        public List<Object> create() {
            return new ArrayList<>();
        }
    };

    @SuppressWarnings("unchecked")
    public static <T> CollectionFactory<List<T>> listFactory() {
        return (CollectionFactory<List<T>>) (CollectionFactory<?>) LIST_FACTORY;
    }

    private static final CollectionFactory<Set<Object>> SET_FACTORY = new CollectionFactory<Set<Object>>() {

        @Override
        public Set<Object> create() {
            return new HashSet<Object>();
        }
    };
    
    private static final CollectionFactory<SortedSet<Object>> SORTED_SET_FACTORY = new CollectionFactory<SortedSet<Object>>() {
        
        @Override
        public SortedSet<Object> create() {
            return new TreeSet<Object>();
        }
    };

    @SuppressWarnings("unchecked")
    public static <T> CollectionFactory<Set<T>> setFactory() {
        return (CollectionFactory<Set<T>>) (CollectionFactory<?>) SET_FACTORY;
    }
    
    @SuppressWarnings("unchecked")
    public static <T> CollectionFactory<SortedSet<T>> sortedSetFactory() {
        return (CollectionFactory<SortedSet<T>>) (CollectionFactory<?>) SORTED_SET_FACTORY;
    }

    /**
     * Private constructor to avoid instantiation of this class.
     */
    private CollectionUtils() {
    }

    /**
     * Copies a collection into an array.
     * 
     * @param collection
     *        is the collection that should be copied to an array
     * @param type
     *        is the class of the elements in the collection
     * @return the array copy
     */
    public static <T> T[] toArray(final Collection<T> collection,
            final Class<T> type) {

        final int size = collection.size();
        @SuppressWarnings("unchecked")
        final T[] array = (T[]) Array.newInstance(type, size);
        collection.toArray(array);

        return array;
    }

    /**
     * Returns an arbitrary element from the given collection.
     * 
     * @param collection
     *        to retrieve the element from
     * @return an arbitrary element or <tt>null</tt> if the collection is empty
     */
    public static <T> T getAny(final Collection<T> collection) {
        final Iterator<T> iterator = collection.iterator();
        if (iterator.hasNext()) {
            return iterator.next();
        }

        return null;
    }

    /**
     * Unions the two given arrays in bag logic. The two arrays must be sorted and the union-elements will be added in
     * sorted order to the given collector.
     * 
     * @param collector
     *        is the {@link Collection} to which the unioned element will be written
     * @param a
     *        is the first union operand
     * @param b
     *        is the second union operand
     */
    public static <T extends Comparable<T>> void unionAll(
            final Collection<T> collector, final T[] a, final T[] b) {

        int index1 = 0;
        int index2 = 0;

        while (true) {
            if (index1 >= a.length) {
                copyRemainder(b, index2, collector);
                break;
            } else if (index2 >= b.length) {
                copyRemainder(a, index1, collector);
                break;
            } else {
                final T candidate1 = a[index1];
                final T candidate2 = b[index2];

                final int comparison = candidate1.compareTo(candidate2);

                if (comparison < 0) {
                    collector.add(candidate1);
                    index1++;
                } else if (comparison > 0) {
                    collector.add(candidate2);
                    index2++;
                } else {
                    collector.add(candidate2);
                    index1++;
                    index2++;
                }
            }
        }
    }

    /**
     * Intersects the two given arrays in bag logic. The two arrays must be sorted and the intersection elements will be
     * added in sorted order to the given collector.
     *
     * @param collector
     *        is the {@link Collection} to which the intersected element will be written
     * @param a
     *        is the first union operand
     * @param b
     *        is the second union operand
     */
    public static <T extends Comparable<T>> void intersectAll(
            final List<T> collector, final T[] a, final T[] b) {

        int index1 = 0;
        int index2 = 0;

        while (true) {
            if (index1 >= a.length || index2 >= b.length) {
                break;

            } else {
                final T candidate1 = a[index1];
                final T candidate2 = b[index2];

                final int comparison = candidate1.compareTo(candidate2);
                if (comparison < 0) {
                    index1++;
                } else if (comparison > 0) {
                    index2++;
                } else {
                    collector.add(candidate2);
                    index1++;
                    index2++;
                }
            }
        }
    }


    /**
     * Calculates the intersect size of two sorted arrays.
     *
     * @param a
     *        is the potentially including array
     * @param b
     *        is the potentially included array
     */
    public static <T extends Comparable<T>> int intersectionSize(final T[] a, final T[] b) {

        int index1 = 0;
        int index2 = 0;
        int intersectionSize = 0;

        while (true) {
            if (index1 >= a.length || index2 >= b.length) {
                break;

            } else {
                final T candidate1 = a[index1];
                final T candidate2 = b[index2];

                final int comparison = candidate1.compareTo(candidate2);
                if (comparison < 0) {
                    index1++;
                } else if (comparison > 0) {
                    index2++;
                } else {
                    intersectionSize++;
                    index1++;
                    index2++;
                }
            }
        }

        return intersectionSize;
    }

    /**
     * Adds all elements of the given array to the collector, starting from the given index.
     * 
     * @param sourceArray
     *        is the source array to copy from
     * @param startIndex
     *        is an index in the array from which the copying shall start
     * @param collector
     *        collects the copied elements
     */
    private static <T> void copyRemainder(final T[] sourceArray,
            final int startIndex, final Collection<T> collector) {

        for (int index = startIndex; index < sourceArray.length; index++) {
            collector.add(sourceArray[index]);
        }
    }

    /**
     * Inserts the specified key-value pair into the map by storing values in a list.
     * 
     * @param map
     *        is the data structure that retains the key-value pairs
     * @param key
     *        is the key of the key-value pair
     * @param value
     *        is the value of the key-value pair
     */
    public static <K, V> void putIntoList(final Map<K, List<V>> map, final K key, final V value) {
        put(map, key, value, CollectionUtils.<V> listFactory());
    }

    /**
     * Inserts the specified key-value pair into the map by storing values in a set.
     * 
     * @param map
     *        is the data structure that retains the key-value pairs
     * @param key
     *        is the key of the key-value pair
     * @param value
     *        is the value of the key-value pair
     */
    public static <K, V> void putIntoSet(final Map<K, Set<V>> map, final K key, final V value) {
        put(map, key, value, CollectionUtils.<V> setFactory());
    }
    
    /**
     * Inserts the specified key-value pair into the map by storing values in a sorted set.
     * 
     * @param map
     *        is the data structure that retains the key-value pairs
     * @param key
     *        is the key of the key-value pair
     * @param value
     *        is the value of the key-value pair
     */
    public static <K, V> void putIntoSortedSet(final Map<K, SortedSet<V>> map, final K key, final V value) {
        put(map, key, value, CollectionUtils.<V> sortedSetFactory());
    }

    /**
     * Inserts the specified key-value pair into the map by storing values in a collection.
     * 
     * @param map
     *        is the data structure that retains the key-value pairs
     * @param key
     *        is the key of the key-value pair
     * @param value
     *        is the value of the key-value pair
     * @param factory
     *        is a factory that creates {@link Collection} objects so that multiple values can be associated with a
     *        single key
     */
    public static <K, V, C extends Collection<V>> void put(final Map<K, C> map, final K key, final V value,
            final CollectionFactory<C> factory) {
        C values = map.get(key);
        if (values == null) {
            values = factory.create();
            map.put(key, values);
        }
        values.add(value);
    }

}

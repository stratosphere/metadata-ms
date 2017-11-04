package de.hpi.isg.mdms.model.util;

import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Target;

import java.util.Arrays;

/**
 * Utilities for managing {@link Target} IDs for {@link Constraint}s.
 */
public class ReferenceUtils {

    /**
     * Sorts the {@code dep} array and re-plays any manipulation on the {@code ref} array. In addition, {@code ref}
     * is used to break ties on {@code dep}. This can be used to create canonical instances where the depenent
     * {@link Column}s are sorted.
     *
     * @param dep the first array
     * @param ref the second array
     */
    public static void coSort(int[] dep, int[] ref) {
        for (int j = ref.length - 1; j > 0; j--) {
            for (int i = j - 1; i >= 0; i--) {
                if (dep[i] > dep[j] || (dep[i] == dep[j] && ref[i] > ref[j])) {
                    swap(dep, i, j);
                    swap(ref, i, j);
                }
            }
        }
    }

    private static void swap(int[] array, int i, int j) {
        int temp = array[i];
        array[i] = array[j];
        array[j] = temp;
    }

    public static boolean isSorted(int[] ids) {
        for (int i = 1; i < ids.length; i++) {
            if (ids[i - 1] > ids[i]) return false;
        }
        return true;
    }

    public static void ensureSorted(int[] ids) {
        if (!isSorted(ids)) Arrays.sort(ids);
    }

    /**
     * Convert an array of {@link Target}s to an array of their IDs.
     *
     * @param targets the {@link Target}s
     * @return the ID array
     */
    public static <T extends Target> int[] toIntArray(T[] targets) {
        int[] intArray = new int[targets.length];
        for (int i = 0; i < targets.length; i++) {
            intArray[i] = targets[i].getId();
        }
        return intArray;
    }

}

package de.hpi.isg.mdms.java.fk.feature;

import de.hpi.isg.mdms.java.fk.Instance;

import java.util.Collection;

/**
 * Super class for various feature classes.
 * @author Lan Jiang
 */
abstract public class Feature {

    abstract public void calcualteFeatureValue(Collection<Instance> instanceCollection);
}

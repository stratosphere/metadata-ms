package de.hpi.isg.metadata_store.domain;

import java.io.Serializable;
import java.util.Map;

import de.hpi.isg.metadata_store.domain.common.Identifiable;
import de.hpi.isg.metadata_store.domain.targets.Column;

/**
 * This interface is the super type for all kinds of constraints that can hold
 * on data represented in the {@link MetadataStore}. All constraints embody a
 * {@link Map} with keys and values. Further they reference {@link Target}s via
 * {@link TargetReference}s. E.g. a constraint can hold on a single
 * {@link Column}, its data type for example. Further constraints can have
 * multiple {@link Target}s in their {@link TargetReference} like an IND, FD,
 * etc. Sub types may store additional information outside of the included
 * {@link Map} fore easier use.
 *
 */
public interface Constraint extends Identifiable, Serializable {

    public Map<Object, Object> getProperties();

    public TargetReference getTargetReference();

}

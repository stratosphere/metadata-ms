package de.hpi.isg.metadata_store.domain;

import java.io.Serializable;

import de.hpi.isg.metadata_store.domain.common.Identifiable;
import de.hpi.isg.metadata_store.domain.common.Named;

public interface Target extends Identifiable, Named, Serializable {
    public Location getLocation();
}

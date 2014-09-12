package de.hpi.isg.metadata_store.domain;

import de.hpi.isg.metadata_store.domain.common.Identifiable;
import de.hpi.isg.metadata_store.domain.common.Named;

public interface ITarget extends Identifiable, Named{
	public ILocation getLocation();
}

package de.hpi.isg.mdms.model.experiment;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import de.hpi.isg.mdms.model.common.AbstractIdentifiable;
import de.hpi.isg.mdms.model.experiment.Algorithm;
import de.hpi.isg.mdms.model.experiment.Experiment;

public class DefaultAlgorithm extends AbstractIdentifiable implements Algorithm{

	private static final long serialVersionUID = 2877105415580520235L;
	private final Set<Experiment> experiments;
	
	private final String name;
	
	public DefaultAlgorithm(int id, String name, Set<Experiment> experiments) {
		super(id);
		this.experiments = experiments;
		this.name = name;
	}

	@Override
	public Collection<Experiment> getExperiments() {
		return Collections.unmodifiableCollection(this.experiments);
	}

	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public void addExperiment(Experiment experiment) {
		this.experiments.add(experiment);
		
	}
	
	

}

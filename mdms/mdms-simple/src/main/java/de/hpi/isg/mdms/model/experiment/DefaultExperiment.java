package de.hpi.isg.mdms.model.experiment;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import de.hpi.isg.mdms.model.DefaultMetadataStore;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.common.AbstractIdentifiable;
import de.hpi.isg.mdms.model.common.ExcludeHashCodeEquals;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.experiment.Algorithm;
import de.hpi.isg.mdms.model.experiment.Experiment;

/**
 * The default {@link de.hpi.isg.mdms.model.experiment.Experiment} implementation that is used by the in-memory {@link de.hpi.isg.mdms.model.DefaultMetadataStore}.
 * 
 * @author susanne
 *
 */

public class DefaultExperiment extends AbstractIdentifiable implements Experiment{

	private static final long serialVersionUID = 5894427384713010467L;
		private final Set<ConstraintCollection> constraintsCollections;
	    private final Algorithm algorithm;
	    private final Map<String, String> parameters;
		private final Set<String> errors_exceptions;
	    
	    private String description;
	    private long executionTime;
	    
	    @ExcludeHashCodeEquals
	    private final DefaultMetadataStore metadataStore;

	    
	public DefaultExperiment(DefaultMetadataStore metadataStore, int id, Algorithm algorithm, Set<ConstraintCollection> constraintCollections,
			Map<String, String> parameters, Set<String> errors_exceptions) {
		super(id);
		this.metadataStore = metadataStore;
		this.algorithm = algorithm;
		this.constraintsCollections = constraintCollections;
		this.parameters = parameters;
		this.errors_exceptions = errors_exceptions;
		
	}

	@Override	
	public String getDescription() {
        return description;
    }

    @Override
    public void setDescription(String description) {
        this.description = description;
    }
	
	@Override
	public Map<String, String> getParameters() {
		return this.parameters;
	}

	@Override
	public Collection<String> getErrorsExceptions() {
		return Collections.unmodifiableCollection(this.errors_exceptions);
	}

	@Override
	public Algorithm getAlgorithm() {
		return this.algorithm;
	}

	@Override
	public Collection<ConstraintCollection> getConstraintCollections() {
		return Collections.unmodifiableCollection(this.constraintsCollections);
	}

	@Override
	public void add(ConstraintCollection constraintCollection) {
		this.constraintsCollections.add(constraintCollection);		
	}

	@Override
	public MetadataStore getMetadataStore() {
		return this.metadataStore;
	}

	@Override
	public Long getExecutionTime() {
		return this.executionTime;
	}

	@Override
	public void setExecutionTime(long executionTime) {
		this.executionTime =executionTime;
	}

	@Override
	public void addParameter(String key, String value) {
		this.parameters.put(key, value);
		
	}

	
	
}

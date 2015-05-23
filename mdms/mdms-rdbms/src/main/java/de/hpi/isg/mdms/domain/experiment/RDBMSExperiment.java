package de.hpi.isg.mdms.domain.experiment;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.common.AbstractIdentifiable;
import de.hpi.isg.mdms.model.common.ExcludeHashCodeEquals;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.experiment.Algorithm;
import de.hpi.isg.mdms.model.experiment.Experiment;
import de.hpi.isg.mdms.rdbms.SQLInterface;

public class RDBMSExperiment extends AbstractIdentifiable implements Experiment{
	
	private static final long serialVersionUID = -7269775415872860403L;

	private static final Logger LOGGER = LoggerFactory.getLogger(RDBMSExperiment.class);

	private  Set<ConstraintCollection> constraintCollections = new HashSet<ConstraintCollection>();
    private Algorithm algorithm;
    private Map<String, String> parameters = new HashMap<String, String>();
	private Set<String> errors_exceptions = new HashSet<String>();
    
    private String description;
    private long executionTime;
    
    @ExcludeHashCodeEquals
    private final SQLInterface sqlInterface;

	public RDBMSExperiment(int id, String description, Long executionTime, Algorithm algorithm, Map<String, String> parameters, Set<String> errors, SQLInterface sqlInterface) {
		super(id);
		this.description = description;
		this.executionTime = executionTime;
		this.algorithm = algorithm;
		this.parameters = parameters;
		this.errors_exceptions = errors;
		this.sqlInterface = sqlInterface;
	}

	public RDBMSExperiment(int id, String description, Long executionTime, Algorithm algorithm, SQLInterface sqlInterface) {
		super(id);
		this.description = description;
		this.executionTime = executionTime;
		this.algorithm = algorithm;
		this.sqlInterface = sqlInterface;
	}	
	
	public RDBMSExperiment(int id, String description, Algorithm algorithm, SQLInterface sqlInterface) {
		super(id);
		this.description = description;
		this.algorithm = algorithm;
		this.sqlInterface = sqlInterface;
	}

	@Override
	public String getDescription() {
		return this.description;
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
	public void addParameter(String key, String value) {
		this.parameters.put(key, value);
		this.sqlInterface.addParameterToExperiment(this, key, value);		
	}

	@Override
	public Long getExecutionTime() {
		return this.executionTime;
	}

	@Override
	public void setExecutionTime(long executionTime) {
		this.executionTime = executionTime;
		this.sqlInterface.setExecutionTimeToExperiment(this, executionTime);
		
	}

	@Override
	public Collection<ConstraintCollection> getConstraintCollections() {
		ensureConstraintCollectionsLoaded();
		return this.constraintCollections;
	}

    private void ensureConstraintCollectionsLoaded() {
        if (this.constraintCollections == null) {
            this.constraintCollections = this.sqlInterface.getAllConstraintCollectionsForExperiment(this);
        }
    }

	@Override
	public void add(ConstraintCollection constraintCollection) {
		this.constraintCollections = null;
		this.sqlInterface.addConstraintCollection(constraintCollection);
		
	}

	@Override
	public MetadataStore getMetadataStore() {
		return this.sqlInterface.getMetadataStore();
	}

}

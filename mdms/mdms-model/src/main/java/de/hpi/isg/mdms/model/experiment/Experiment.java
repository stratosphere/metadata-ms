package de.hpi.isg.mdms.model.experiment;

import java.util.Collection;
import java.util.Map;

import de.hpi.isg.mdms.model.common.Described;
import de.hpi.isg.mdms.model.common.Identifiable;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.MetadataStore;

/**
 * A {@link Experiment} represents an algorithm execution and stores relevant metadata for this execution.
 * 
 * @author susanne
 *
 */

public interface Experiment extends Identifiable, Described {

	
    Map<String, String> getParameters();
    
    Collection<Annotation> getAnnotations();
	
    /**
     * This function returns the {@link Algorithm} of this experiment.
     * 
     * @return {@link Algorithm} .
     */
    public Algorithm getAlgorithm();
    

    /**
     * This function adds a key-value-pair to the parameters of this experiment.
     * 
     */
    public void addParameter(String key, String value);

    
    /**
     * This function returns the execution time of this experiment.
     * 
     * @return execution time of experiment .
     */
    public Long getExecutionTime();

    /**
     * This function sets the execution time of this experiment.
     * 
     * @param execution time of experiment .
     */
    public void setExecutionTime(long executionTime);

    
    /**
     * This functions returns the {@link de.hpi.isg.mdms.model.constraints.ConstraintCollection} connected to this {@link Experiment}.
     * 
     * @return {@link Collection} of {@link de.hpi.isg.mdms.model.constraints.ConstraintCollection}s.
     */
    public Collection<ConstraintCollection<? extends Constraint>> getConstraintCollections();

    /**
     * Adds a new {@link de.hpi.isg.mdms.model.constraints.ConstraintCollection} to this collection.
     * 
     * @param constraintCollection
     *        The {@link de.hpi.isg.mdms.model.constraints.ConstraintCollection} to add.
     */
    public void add(ConstraintCollection<? extends Constraint> constraintCollection);

    /**
     * This function returns the {@link de.hpi.isg.mdms.model.MetadataStore} this collection belongs to.
     * 
     * @return the {@link de.hpi.isg.mdms.model.MetadataStore}.
     */
    public MetadataStore getMetadataStore();
    
    /**
     * Adds an annotation to the existing Experiment
     * @param annotation tag
     * @parm annotation text
     */
	void addAnnotation(String tag, String text);
	
	/**
	 * Returns the timestamp of the experiment
	 */
	String getTimestamp();
}

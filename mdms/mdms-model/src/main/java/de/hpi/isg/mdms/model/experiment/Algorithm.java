package de.hpi.isg.mdms.model.experiment;

import java.util.Collection;

import de.hpi.isg.mdms.model.common.Identifiable;


/**
 * An {@link Algorithm} represents a certain algorithm. Each execution of the algorithm represents a {@link Experiment}.
 * 
 * @author susanne
 *
 */
public interface Algorithm extends Identifiable{
	
	
    /**
     * This functions returns the {@link Experiment}s representing the executions of this {@link Algorithm}.
     * 
     * @return {@link Collection} of {@link Experiment}s.
     */
    public Collection<Experiment> getExperiments();

    /**
     * This functions returns the name of this {@link Algorithm}.
     * 
     * @return name
     */
    public String getName();
    
    
    /**
     * This functions adds an {@link Experiment} to the {@link Algorithm}.
     * 
     * @param {@link Experiment}
     */
    public void addExperiment(Experiment experiment);

    
}

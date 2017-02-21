package de.hpi.isg.mdms.domain.experiment;

import de.hpi.isg.mdms.exceptions.MetadataStoreException;
import de.hpi.isg.mdms.model.common.AbstractIdentifiable;
import de.hpi.isg.mdms.model.common.ExcludeHashCodeEquals;
import de.hpi.isg.mdms.model.experiment.Algorithm;
import de.hpi.isg.mdms.model.experiment.Experiment;
import de.hpi.isg.mdms.rdbms.SQLInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collection;


/**
 * The implementation of a {@link Algorithm} that is used in {@link de.hpi.isg.mdms.domain.RDBMSMetadataStore}s.
 *
 * @author susanne
 */

public class RDBMSAlgorithm extends AbstractIdentifiable implements Algorithm {

    private static final long serialVersionUID = -2911473574180511468L;

    private static final Logger LOGGER = LoggerFactory.getLogger(RDBMSAlgorithm.class);

    private String name;

    private Collection<Experiment> experiments = null;

    @ExcludeHashCodeEquals
    private SQLInterface sqlInterface;


    public RDBMSAlgorithm(int id, String name, SQLInterface sqlInterface) {
        super(id);
        this.name = name;
        this.sqlInterface = sqlInterface;

    }

    @Override
    public Collection<Experiment> getExperiments() {
        ensureExperimentsLoaded();
        return experiments;
    }

    private void ensureExperimentsLoaded() {
        if (this.experiments == null) {
            try {
                this.experiments = this.sqlInterface.getAllExperimentsForAlgorithm(this);
            } catch (SQLException e) {
                throw new MetadataStoreException(e);
            }
        }
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public void addExperiment(Experiment experiment) {
        this.experiments = null;

        try {
            this.sqlInterface.writeExperiment((RDBMSExperiment) experiment);
        } catch (SQLException e) {
            throw new MetadataStoreException(e);
        }

    }

}

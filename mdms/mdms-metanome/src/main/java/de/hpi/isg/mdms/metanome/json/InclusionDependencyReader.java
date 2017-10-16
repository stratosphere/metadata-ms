package de.hpi.isg.mdms.metanome.json;

import de.hpi.isg.mdms.metanome.DependencyResultReceiver;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.results.InclusionDependency;

/**
 * Parser for {@link InclusionDependency} constraints.
 */
public class InclusionDependencyReader extends AbstractJsonReader<InclusionDependency> {

    @Override
    protected void feed(InclusionDependency metanomeConstraint, DependencyResultReceiver<?> resultReceiver) throws CouldNotReceiveResultException {
        resultReceiver.receiveResult(metanomeConstraint);
    }

    @Override
    protected Class<InclusionDependency> getMetanomeTypeClass() {
        return InclusionDependency.class;
    }
}

package de.hpi.isg.mdms.tools.metanome.json;

import de.hpi.isg.mdms.tools.metanome.DependencyResultReceiver;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.results.FunctionalDependency;

/**
 * Parser for {@link FunctionalDependency} constraints.
 */
public class FunctionalDependencyReader extends AbstractJsonReader<FunctionalDependency> {

    @Override
    protected void feed(FunctionalDependency metanomeConstraint, DependencyResultReceiver<?> resultReceiver) throws CouldNotReceiveResultException {
        resultReceiver.receiveResult(metanomeConstraint);
    }

    @Override
    protected Class<FunctionalDependency> getMetanomeTypeClass() {
        return FunctionalDependency.class;
    }
}

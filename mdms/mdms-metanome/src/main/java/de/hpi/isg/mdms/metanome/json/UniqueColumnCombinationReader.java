package de.hpi.isg.mdms.metanome.json;

import de.hpi.isg.mdms.metanome.DependencyResultReceiver;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.results.UniqueColumnCombination;

/**
 * Parser for {@link UniqueColumnCombination} constraints.
 */
public class UniqueColumnCombinationReader extends AbstractJsonReader<UniqueColumnCombination> {


    @Override
    protected void feed(UniqueColumnCombination metanomeConstraint, DependencyResultReceiver<?> resultReceiver) throws CouldNotReceiveResultException {
        resultReceiver.receiveResult(metanomeConstraint);
    }

    @Override
    protected Class<UniqueColumnCombination> getMetanomeTypeClass() {
        return UniqueColumnCombination.class;
    }
}

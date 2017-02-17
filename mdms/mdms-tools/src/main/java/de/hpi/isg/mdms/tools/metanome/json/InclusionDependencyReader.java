package de.hpi.isg.mdms.tools.metanome.json;

import de.hpi.isg.mdms.tools.metanome.DependencyResultReceiver;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.results.InclusionDependency;

import java.util.Collection;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

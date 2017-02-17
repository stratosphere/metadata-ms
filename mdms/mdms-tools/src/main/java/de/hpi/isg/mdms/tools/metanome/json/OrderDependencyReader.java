package de.hpi.isg.mdms.tools.metanome.json;

import de.hpi.isg.mdms.tools.metanome.DependencyResultReceiver;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.results.OrderDependency;

import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Parser for {@link OrderDependency} constraints.
 */
public class OrderDependencyReader extends AbstractJsonReader<OrderDependency> {


    @Override
    protected void feed(OrderDependency metanomeConstraint, DependencyResultReceiver<?> resultReceiver) throws CouldNotReceiveResultException {
        throw new RuntimeException("ODs are not supported, yet.");
    }

    @Override
    protected Class<OrderDependency> getMetanomeTypeClass() {
        return OrderDependency.class;
    }
}

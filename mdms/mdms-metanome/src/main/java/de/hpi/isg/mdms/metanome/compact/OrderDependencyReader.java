package de.hpi.isg.mdms.metanome.compact;

import de.hpi.isg.mdms.metanome.DependencyResultReceiver;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.results.OrderDependency;

/**
 * @author Lan Jiang
 * @since 24/02/2017
 */
public class OrderDependencyReader extends AbstractCompactReader<OrderDependency> {
    @Override
    protected void processLine(String line, DependencyResultReceiver<?> resultReceiver) {
        try {
            throw new CouldNotReceiveResultException("ODs are not supported.");
        } catch (CouldNotReceiveResultException e) {
            throw new RuntimeException(String.format("Could not process \"{}\".", line), e);
        }
    }
}

package de.hpi.isg.mdms.tools.metanome.friendly;

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
public class OrderDependencyReader extends AbstractFriendlyReader<OrderDependency> {

    @Override
    protected void processLine(String line, DependencyResultReceiver<?> resultReceiver) {
        toOD(line).forEach(od -> {
            try {
//                resultReceiver.receiveResult(od);
                throw new CouldNotReceiveResultException("ODs are not supported.");
            } catch (CouldNotReceiveResultException e) {
                throw new RuntimeException(String.format("Could not process \"{}\".", line), e);
            }
        });
    }

    private Stream<OrderDependency> toOD(String line) {
        String[] split = line.split(Pattern.quote("]") + " ~~> " + Pattern.quote("["));
        String[] rhsSplit = split[1].split(Pattern.quote("]") + ", " + Pattern.quote("["));
        ColumnPermutation lhs = toColumnPermutation(split[0].replaceAll(Pattern.quote("[") + "|" + Pattern.quote("]"), ""));
        return Arrays.stream(rhsSplit)
                .map(rhs -> new OrderDependency(lhs,
                        toColumnPermutation(rhs.replaceAll(Pattern.quote("[") + "|" + Pattern.quote("]"), "")),
                        OrderDependency.OrderType.LEXICOGRAPHICAL,
                        OrderDependency.ComparisonOperator.SMALLER_EQUAL));
    }
}

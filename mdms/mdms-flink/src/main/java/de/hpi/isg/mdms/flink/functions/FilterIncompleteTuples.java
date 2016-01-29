package de.hpi.isg.mdms.flink.functions;


import de.hpi.isg.mdms.flink.data.Tuple;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * This function retains only those {@link Tuple}s that have no empty or {@code null} field.
 * 
 * @author Sebastian Kruse
 */
public class FilterIncompleteTuples implements FilterFunction<Tuple> {

    @Override
    public boolean filter(Tuple value) throws Exception {
        return !value.hasEmptyOrNullField();
    }
}

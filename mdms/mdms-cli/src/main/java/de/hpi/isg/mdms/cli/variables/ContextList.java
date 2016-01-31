package de.hpi.isg.mdms.cli.variables;


import de.hpi.isg.mdms.cli.exceptions.CliException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * A {@link ContextList} maps variable names to {@link ContextObject}s. This implies that {@link ContextList}s can be
 * nested.
 */
public class ContextList extends ContextValue<List<ContextObject>> implements Iterable<ContextObject> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ContextList.class);

    private List<ContextObject> variables = new LinkedList<>();

    public void add(ContextObject o) {
        this.variables.add(o);
    }

    @Override
    public String toParseableString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        String separator = "";
        for (ContextObject o : this) {
            sb.append(separator).append(o.toParseableString());
            separator=",";
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public String toReadableString() {
        return toParseableString();
    }

    @Override
    public List<ContextObject> getValue() {
        return Collections.unmodifiableList(this.variables);
    }

    public ContextObject get(int position) throws CliException {
        return this.variables.get(position);
    }

    @Override
    public String toString() {
        return toParseableString();
    }

    @Override
    public Iterator<ContextObject> iterator() {
        return this.variables.iterator();
    }
}

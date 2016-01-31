package de.hpi.isg.mdms.cli.variables;


import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * A {@link ContextReference} is a list of path segments that describe objects in nested {@code Namespace}s.
 */
public class ContextReference implements ContextObject {

    private final List<String> path = new LinkedList<>();

    public ContextReference(String... path) {
        this(Arrays.asList(path));
    }

    public ContextReference(List<String> path) {
        this.path.addAll(path);
    }

    @Override
    public String toParseableString() {
        return "*{" + StringUtils.join(this.path, ".") + "}";
    }

    @Override
    public String toReadableString() {
        return toParseableString();
    }

    @Override
    public boolean isValue() {
        return false;
    }

    @Override
    public boolean isReference() {
        return true;
    }

    @Override
    public boolean isException() {
        return false;
    }

    /**
     * @return the path segments
     */
    public List<String> getPath() {
        return path;
    }

    /**
     * @return the last segment of the path
     * @see #getPath()
     */
    public String getLastSegment() {
        return this.path.get(this.path.size() - 1);
    }
}

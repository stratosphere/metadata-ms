package de.hpi.isg.metadata_store.domain.impl;

import de.hpi.isg.metadata_store.domain.Location;
import de.hpi.isg.metadata_store.domain.common.impl.ExcludeHashCodeEquals;
import de.hpi.isg.metadata_store.domain.factories.SQLInterface;

public class RDBMSTarget extends AbstractTarget {

    private static final long serialVersionUID = -2207050281912169066L;

    @ExcludeHashCodeEquals
    private SQLInterface sqlInterface;

    public RDBMSTarget(RDBMSMetadataStore observer, int id, String name, Location location) {
        super(observer, id, name, location);
        this.sqlInterface = observer.getSQLInterface();
    }

    public SQLInterface getSqlInterface() {
        return sqlInterface;
    }

    @Override
    public String toString() {
        return String.format("Target[%s, %s, %08x]", this.getName(), this.getLocation(), this.getId());
    }

}

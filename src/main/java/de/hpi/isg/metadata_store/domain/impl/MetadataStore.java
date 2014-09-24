package de.hpi.isg.metadata_store.domain.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import de.hpi.isg.metadata_store.domain.IConstraint;
import de.hpi.isg.metadata_store.domain.IMetadataStore;
import de.hpi.isg.metadata_store.domain.ITarget;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractIdentifiableAndNamed;
import de.hpi.isg.metadata_store.domain.targets.ISchema;
import de.hpi.isg.metadata_store.exceptions.NotAllTargetsInStoreException;

public class MetadataStore extends AbstractIdentifiableAndNamed implements IMetadataStore {

    private static final long serialVersionUID = -1214605256534100452L;

    private Collection<ISchema> schemas;
    private Collection<IConstraint> constraints;
    private Collection<ITarget> allTargets;

    public MetadataStore(long id, String name) {
	super(id, name);
	this.schemas = new HashSet<ISchema>();
	this.constraints = new HashSet<IConstraint>();
	this.allTargets = new HashSet<>();
    }

    @Override
    public Collection<ISchema> getSchemas() {
	return schemas;
    }

    @Override
    public Collection<IConstraint> getConstraints() {
	return Collections.unmodifiableCollection(constraints);
    }

    @Override
    public Collection<ITarget> getAllTargets() {
	return Collections.unmodifiableCollection(allTargets);
    }

    @Override
    public void addConstraint(IConstraint constraint) {
	for (ITarget target : constraint.getTargetReference().getAllTargets()) {
	    if (!this.allTargets.contains(target)) {
		throw new NotAllTargetsInStoreException(target);
	    }

	}

    }

    @Override
    public void update(Object message) {
	if (message instanceof ITarget) {
	    this.allTargets.add((ITarget) message);
	}
    }

    // ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Static methods
    // ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static IMetadataStore getMetadataStoreForId(File dir, long id) throws IOException, ClassNotFoundException {

	FileInputStream fin = new FileInputStream(dir.getAbsolutePath() + File.separator + id + ".ms");
	ObjectInputStream ois = new ObjectInputStream(fin);
	IMetadataStore metadataStore = (IMetadataStore) ois.readObject();
	ois.close();

	return metadataStore;
    }

    public static void saveMetadataStore(File dir, IMetadataStore metadataStore) throws IOException {
	FileOutputStream fout = new FileOutputStream(dir.getAbsolutePath() + File.separator + metadataStore.getId()
		+ ".ms");
	ObjectOutputStream oos = new ObjectOutputStream(fout);
	oos.writeObject(metadataStore);
	oos.close();
    }

    @Override
    public String toString() {
	return "MetadataStore [schemas=" + schemas + ", constraints=" + constraints + ", allTargets=" + allTargets
		+ ", getSchemas()=" + getSchemas() + ", getConstraints()=" + getConstraints() + ", getId()=" + getId()
		+ ", getName()=" + getName() + "]";
    }
}

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

import de.hpi.isg.metadata_store.domain.Constraint;
import de.hpi.isg.metadata_store.domain.MetadataStore;
import de.hpi.isg.metadata_store.domain.Target;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractIdentifiableAndNamed;
import de.hpi.isg.metadata_store.domain.targets.Schema;
import de.hpi.isg.metadata_store.exceptions.NotAllTargetsInStoreException;

public class DefaultMetadataStore extends AbstractIdentifiableAndNamed implements MetadataStore {

    private static final long serialVersionUID = -1214605256534100452L;

    private Collection<Schema> schemas;
    private Collection<Constraint> constraints;
    private Collection<Target> allTargets;

    public DefaultMetadataStore(long id, String name) {
	super(id, name);
	this.schemas = new HashSet<Schema>();
	this.constraints = new HashSet<Constraint>();
	this.allTargets = new HashSet<>();
    }

    @Override
    public Collection<Schema> getSchemas() {
	return schemas;
    }

    @Override
    public Collection<Constraint> getConstraints() {
	return Collections.unmodifiableCollection(constraints);
    }

    @Override
    public Collection<Target> getAllTargets() {
	return Collections.unmodifiableCollection(allTargets);
    }

    @Override
    public void addConstraint(Constraint constraint) {
	for (Target target : constraint.getTargetReference().getAllTargets()) {
	    if (!this.allTargets.contains(target)) {
		throw new NotAllTargetsInStoreException(target);
	    }

	}

    }

    @Override
    public void update(Object message) {
	if (message instanceof Target) {
	    this.allTargets.add((Target) message);
	}
    }

    // ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Static methods
    // ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static MetadataStore getMetadataStoreForId(File dir, long id) throws IOException, ClassNotFoundException {

	FileInputStream fin = new FileInputStream(dir.getAbsolutePath() + File.separator + id + ".ms");
	ObjectInputStream ois = new ObjectInputStream(fin);
	MetadataStore metadataStore = (MetadataStore) ois.readObject();
	ois.close();

	return metadataStore;
    }

    public static void saveMetadataStore(File dir, MetadataStore metadataStore) throws IOException {
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

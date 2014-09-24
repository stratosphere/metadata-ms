package de.hpi.isg.metadata_store.domain.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
import de.hpi.isg.metadata_store.domain.common.impl.AbstractHashCodeAndEquals;
import de.hpi.isg.metadata_store.domain.targets.Schema;
import de.hpi.isg.metadata_store.exceptions.MetadataStoreNotFoundException;
import de.hpi.isg.metadata_store.exceptions.NotAllTargetsInStoreException;

public class DefaultMetadataStore extends AbstractHashCodeAndEquals implements MetadataStore {

    private static final long serialVersionUID = -1214605256534100452L;

    private Collection<Schema> schemas;
    private Collection<Constraint> constraints;
    private Collection<Target> allTargets;

    public DefaultMetadataStore() {
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

    public static MetadataStore getMetadataStore(File file) throws MetadataStoreNotFoundException {

	FileInputStream fin;
	try {
	    fin = new FileInputStream(file);
	    ObjectInputStream ois = new ObjectInputStream(fin);
	    MetadataStore metadataStore = (MetadataStore) ois.readObject();
	    ois.close();
	    return metadataStore;
	} catch (IOException | ClassNotFoundException e) {
	    throw new MetadataStoreNotFoundException(e);
	}

    }

    public static void saveMetadataStore(File file, MetadataStore metadataStore) throws IOException {
	FileOutputStream fout = new FileOutputStream(file);
	ObjectOutputStream oos = new ObjectOutputStream(fout);
	oos.writeObject(metadataStore);
	oos.close();
    }

    @Override
    public String toString() {
	return "MetadataStore [schemas=" + schemas + ", constraints=" + constraints + ", allTargets=" + allTargets
		+ ", getSchemas()=" + getSchemas() + ", getConstraints()=" + getConstraints() + "]";
    }

    public static MetadataStore getOrCreateAndSaveMetadataStore(File file) throws IOException {
	try {
	    return DefaultMetadataStore.getMetadataStore(file);
	} catch (MetadataStoreNotFoundException e) {
	    MetadataStore metadataStore = new DefaultMetadataStore();
	    DefaultMetadataStore.saveMetadataStore(file, metadataStore);
	    return metadataStore;
	}
    }
}

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
import de.hpi.isg.metadata_store.domain.common.impl.AbstractHashCodeAndEquals;
import de.hpi.isg.metadata_store.domain.targets.Schema;
import de.hpi.isg.metadata_store.exceptions.MetadataStoreNotFoundException;
import de.hpi.isg.metadata_store.exceptions.NotAllTargetsInStoreException;

public class DefaultMetadataStore extends AbstractHashCodeAndEquals implements MetadataStore {

    public static MetadataStore getMetadataStore(File file) throws MetadataStoreNotFoundException {

	FileInputStream fin;
	try {
	    fin = new FileInputStream(file);
	    final ObjectInputStream ois = new ObjectInputStream(fin);
	    final MetadataStore metadataStore = (MetadataStore) ois.readObject();
	    ois.close();
	    return metadataStore;
	} catch (IOException | ClassNotFoundException e) {
	    throw new MetadataStoreNotFoundException(e);
	}

    }

    public static MetadataStore getOrCreateAndSaveMetadataStore(File file) throws IOException {
	try {
	    return DefaultMetadataStore.getMetadataStore(file);
	} catch (final MetadataStoreNotFoundException e) {
	    final MetadataStore metadataStore = new DefaultMetadataStore();
	    DefaultMetadataStore.saveMetadataStore(file, metadataStore);
	    return metadataStore;
	}
    }

    public static void saveMetadataStore(File file, MetadataStore metadataStore) throws IOException {
	final FileOutputStream fout = new FileOutputStream(file);
	final ObjectOutputStream oos = new ObjectOutputStream(fout);
	oos.writeObject(metadataStore);
	oos.close();
    }

    private static final long serialVersionUID = -1214605256534100452L;

    private final Collection<Schema> schemas;

    private final Collection<Constraint> constraints;

    private final Collection<Target> allTargets;

    public DefaultMetadataStore() {
	this.schemas = new HashSet<Schema>();
	this.constraints = new HashSet<Constraint>();
	this.allTargets = new HashSet<>();
    }

    @Override
    public void addConstraint(Constraint constraint) {
	for (final Target target : constraint.getTargetReference().getAllTargets()) {
	    if (!this.allTargets.contains(target)) {
		throw new NotAllTargetsInStoreException(target);
	    }

	}

    }

    @Override
    public void addSchema(Schema schema) {
	this.schemas.add(schema);

    }

    // ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Static methods
    // ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public Collection<Target> getAllTargets() {
	return Collections.unmodifiableCollection(this.allTargets);
    }

    @Override
    public Collection<Constraint> getConstraints() {
	return Collections.unmodifiableCollection(this.constraints);
    }

    @Override
    public Collection<Schema> getSchemas() {
	return this.schemas;
    }

    @Override
    public String toString() {
	return "MetadataStore [schemas=" + this.schemas + ", constraints=" + this.constraints + ", allTargets="
		+ this.allTargets + ", getSchemas()=" + this.getSchemas() + ", getConstraints()="
		+ this.getConstraints() + "]";
    }

    @Override
    public void update(Object message) {
	if (message instanceof Target) {
	    this.allTargets.add((Target) message);
	}
    }
}

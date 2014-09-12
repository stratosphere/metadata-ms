package de.hpi.isg.metadata_store.domain.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.HashSet;

import de.hpi.isg.metadata_store.domain.IMetadataStore;
import de.hpi.isg.metadata_store.domain.common.impl.AbstractIdentifiableAndNamed;
import de.hpi.isg.metadata_store.domain.targets.ISchema;

public class MetadataStore extends AbstractIdentifiableAndNamed implements
		IMetadataStore {

	private static final long serialVersionUID = -1214605256534100452L;

	private Collection<ISchema> schemas;

	public MetadataStore(long id, String name) {
		super(id, name);
		this.schemas = new HashSet<ISchema>();
	}

	@Override
	public Collection<ISchema> getSchemas() {
		return schemas;
	}

	// ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// Static methods
	// ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public static IMetadataStore getMetadataStoreForId(File dir, long id)
			throws IOException, ClassNotFoundException {

		FileInputStream fin = new FileInputStream(dir.getAbsolutePath()
				+ File.separator + id + ".ms");
		ObjectInputStream ois = new ObjectInputStream(fin);
		IMetadataStore metadataStore = (IMetadataStore) ois.readObject();
		ois.close();

		return metadataStore;
	}

	public static void saveMetadataStore(File dir, IMetadataStore metadataStore)
			throws IOException {
		FileOutputStream fout = new FileOutputStream(dir.getAbsolutePath()
				+ File.separator + metadataStore.getId() + ".ms");
		ObjectOutputStream oos = new ObjectOutputStream(fout);
		oos.writeObject(metadataStore);
		oos.close();
	}

}

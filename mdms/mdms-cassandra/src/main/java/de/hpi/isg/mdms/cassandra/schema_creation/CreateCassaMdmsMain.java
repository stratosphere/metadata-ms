package de.hpi.isg.mdms.cassandra.schema_creation;

import de.hpi.isg.mdms.domain.CassaMetadataStore;

public class CreateCassaMdmsMain {
	
	public static void main(String[] args){
		CassaMetadataStore metadatastore = CassaMetadataStore.createNewInstance("172.16.18.18"); //; "localhost"; "172.16.19.209"
		metadatastore.close();
	}

}

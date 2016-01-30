package de.hpi.isg.mdms.flink.readwrite;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * This interface is the counterpart to the {@link RemoteCollectorOutputFormat}
 * and implementations will receive remote results through the collect function.
 * 
 * @param <T>
 *            The type of the records the collector will receive
 */
public interface RemoteCollector<T> extends Remote {

	void collect(T element) throws RemoteException;

	RemoteCollectorConsumer<T> getConsumer() throws RemoteException;

	void setConsumer(RemoteCollectorConsumer<T> consumer)
			throws RemoteException;

}

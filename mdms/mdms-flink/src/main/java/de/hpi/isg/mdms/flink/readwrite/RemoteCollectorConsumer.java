package de.hpi.isg.mdms.flink.readwrite;

/**
 * This interface describes consumers of {@link RemoteCollector} implementations.
 */
public interface RemoteCollectorConsumer<T> {
    void collect(T element);
}

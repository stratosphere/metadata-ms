package de.hpi.isg.mdms.exceptions;

/**
 * This exception represents an issue that happened while operating a {@link de.hpi.isg.mdms.model.MetadataStore}.
 */
public class MetadataStoreException extends RuntimeException {

    public MetadataStoreException() {
    }

    public MetadataStoreException(String message) {
        super(message);
    }

    public MetadataStoreException(String message, Throwable cause) {
        super(message, cause);
    }

    public MetadataStoreException(Throwable cause) {
        super(cause);
    }

    public MetadataStoreException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}

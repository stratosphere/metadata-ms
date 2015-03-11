package de.hpi.isg.metadata_store.domain.common.impl;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This tag interface is used by the MetadataStoreCLI to determine which fields of an object should be displayed in the
 * command-line-interface.
 * 
 * @author fabian
 *
 */

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Printable {

}
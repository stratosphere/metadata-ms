package de.hpi.isg.metadata_store.domain.common.impl;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation can be used by sub classes of
 * {@link AbstractHashCodeAndEquals} to signal that a certain member shall not
 * be taken into account for hashCode() and equals().
 *
 * @author fabian
 *
 */

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ExcludeHashCodeEquals {

}
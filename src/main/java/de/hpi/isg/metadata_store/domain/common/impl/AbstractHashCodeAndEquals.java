package de.hpi.isg.metadata_store.domain.common.impl;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * This class is an abstract convenience class caring for hashCode() and
 * equals(). Extending classes can exclude member variables explicitly be using
 * the {@link ExcludeHashCodeEquals} annotation.
 *
 */
public abstract class AbstractHashCodeAndEquals {
	
	private static Map<Class<?>, Set<String>> excludedFields = new HashMap<>();
	
    @Override
    public boolean equals(Object obj) {
	return EqualsBuilder.reflectionEquals(this, obj, this.getExlcudedFields());
    }

    private Collection<String> getExlcudedFields() {
    Set<String> excludes = excludedFields.get(getClass());
    if (excludes == null) {
    	excludes = new HashSet<>();

		Class<?> clazz = this.getClass();
		while (clazz != null) {
		    for (final Field field : clazz.getDeclaredFields()) {
			final String name = field.getName();
			for (final Annotation a : field.getDeclaredAnnotations()) {
			    if (a instanceof ExcludeHashCodeEquals) {
				excludes.add(name);
			    }
			}
		    }
		    clazz = clazz.getSuperclass();
		}
		
		excludedFields.put(getClass(), excludes);
    }
	return excludes;
    }

    @Override
    public int hashCode() {

	return HashCodeBuilder.reflectionHashCode(this, this.getExlcudedFields());
    }
}

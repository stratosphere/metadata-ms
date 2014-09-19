package de.hpi.isg.metadata_store.domain.common.impl;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashSet;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class AbstractHashCodeAndEquals {
	@Override
	public int hashCode() {

		return HashCodeBuilder.reflectionHashCode(this, getExlcudedFields());
	}

	@Override
	public boolean equals(Object obj) {
		return EqualsBuilder.reflectionEquals(this, obj, getExlcudedFields());
	}

	private Collection<String> getExlcudedFields() {
		Collection<String> excludes = new HashSet<String>();

		Class<?> clazz = this.getClass();
		while (clazz != null) {
			for (Field field : clazz.getDeclaredFields()) {
				String name = field.getName();
				for (Annotation a : field.getDeclaredAnnotations()) {
					if (a instanceof ExcludeHashCodeEquals) {
						excludes.add(name);
					}
				}
			}
			clazz = clazz.getSuperclass();
		}
		return excludes;
	}
}

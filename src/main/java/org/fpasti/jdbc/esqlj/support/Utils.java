package org.fpasti.jdbc.esqlj.support;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * @author Fabrizio Pasti - fabrizio.pasti@gmail.com
 */

public class Utils {
	@SuppressWarnings("rawtypes")
	private static final Map<Class, Function<String, ?>> parsersCollection = new HashMap<>();

	static {
		parsersCollection.put(Long.class, Long::parseLong);
		parsersCollection.put(Integer.class, Integer::parseInt);
		parsersCollection.put(String.class, String::toString);
		parsersCollection.put(Double.class, Double::parseDouble);
		parsersCollection.put(Float.class, Float::parseFloat);
		parsersCollection.put(Boolean.class, Boolean::parseBoolean);
	}

	public static Object toString(Class<?> clazz, String value) {
		switch (clazz.getSimpleName()) {
			case "Boolean":
				return Boolean.getBoolean(value.toLowerCase());
			case "Integer":
				return Integer.getInteger(value);
			default:
				return value;
		}
	}

	public static InputStream getAsciiStreamInternal(String value) {
		if (value == null) {
			return null;
		}
		InputStream is;
		try {
			is = new ByteArrayInputStream(value.getBytes("ASCII"));
		} catch (Exception e) {
			return null;
		}
		return is;
	}

	public static void setPropertyBeanValue(Object objectInstance, String name, String value) {
		String camelCaseName = convertStringFromSnakeToCamel(name);		
		try {
			BeanInfo beanInfo = Introspector.getBeanInfo(objectInstance.getClass());
			PropertyDescriptor propertyDescriptor = Arrays.stream(beanInfo.getPropertyDescriptors())
					.filter(propDescr -> {
						return propDescr.getName().equals(camelCaseName);
					}).findFirst().orElseThrow(() -> new Exception());
					
			propertyDescriptor.getWriteMethod().invoke(objectInstance, parseString(propertyDescriptor.getPropertyType(), value));
		} catch(Exception e) {
			throw new EsRuntimeException(String.format("Cannot set field with name '%s' in object typed '%s'", camelCaseName, objectInstance.getClass().getName()));
		}
	}

	public static void setAttributeInElasticObject(Object objectInstance, String name, String value) {
		Class<?> clazz = objectInstance.getClass();
		String camelCaseMethodName = convertStringFromSnakeToCamel(name);
		try {
			Method method = Arrays.asList(clazz.getDeclaredMethods()).stream().filter(methodName -> {
				return methodName.getName().equals(camelCaseMethodName) && methodName.getParameterCount() == 1;
			}).findFirst().orElseThrow(() -> new Exception());
			method.invoke(objectInstance, parseString(method.getParameterTypes()[0], value));
		} catch(NoSuchMethodException nse) {
			throw new EsRuntimeException(String.format("Cannot find field with name '%s' in object typed '%s'", camelCaseMethodName, objectInstance.getClass().getName()));
		} catch (Exception e) {
			throw new EsRuntimeException(String.format("Cannot set field with name '%s' in object typed '%s'", camelCaseMethodName, objectInstance.getClass().getName()));
		}
	}
	
	@SuppressWarnings("unchecked")
	public static <T> T parseString(Class<T> clazz, String value) {
		return (T) parsersCollection.get(clazz).apply(value);
	}

	public static String convertStringFromSnakeToCamel(String str) {
		while(str.contains("_")) {
        	str = str.replaceFirst("_[a-z]", String.valueOf(Character.toUpperCase(str.charAt(str.indexOf("_") + 1))));
        }
		
		return str;
	}
	
	public static String resolveSetterBySnakCase(String str) {
		while(str.contains("_")) {
        	str = str.replaceFirst("_[a-z]", String.valueOf(Character.toUpperCase(str.charAt(str.indexOf("_") + 1))));
        }
		String camelCase = convertStringFromSnakeToCamel(str);
		return "set".concat(camelCase.substring(0, 1).toUpperCase().concat(camelCase.substring(1)));
	}
}

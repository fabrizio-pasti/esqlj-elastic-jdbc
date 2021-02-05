package org.fpasti.jdbc.esqlj.support;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class Utils {
	
	public static Object toString(Class<?> clazz, String value) {
		switch(clazz.getSimpleName()) {
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
}

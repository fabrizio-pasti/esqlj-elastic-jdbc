package org.fpasti.jdbc.esqlj.elastic.query.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class DataRow {
	public List<Object> data = new ArrayList<Object>();

	public DataRow(Object... values) {
		putAll(values);
	}

	public DataRow(int size) {
		data = Stream.generate(Object::new)
                .limit(size)
                .collect(Collectors.toList());
	}

	public DataRow(List<Object> values) {
		data = values;
	}

	public void put(int index, Object value) {
		data.set(index, value);
	}
	
	private void putAll(Object... values) {
		data = Arrays.asList(values);
	}
	
	
}

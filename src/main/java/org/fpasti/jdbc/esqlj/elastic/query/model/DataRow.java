package org.fpasti.jdbc.esqlj.elastic.query.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class DataRow {
	public List<Object> data = new ArrayList<Object>();

	public DataRow(Object... values) {
		putAll(values);
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

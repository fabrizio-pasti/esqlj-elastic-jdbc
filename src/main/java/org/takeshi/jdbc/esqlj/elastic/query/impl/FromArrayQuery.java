package org.takeshi.jdbc.esqlj.elastic.query.impl;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.takeshi.jdbc.esqlj.elastic.query.AbstractOneShotQuery;

public class FromArrayQuery extends AbstractOneShotQuery {
		
	public FromArrayQuery(String source, List<List<Object>> values, String... columnNames) throws SQLException {
		super(null, source, columnNames);
		init(values);
	}

	public void init(List<List<Object>> values) throws SQLException {
		values.forEach(rowData -> {
			Map<String, Object> data = new HashMap<String, Object>();
			for(int idx = 0; idx < this.getColumnNames().size(); idx++) {
				data.put(this.getColumnNames().get(idx), rowData.get(idx));
			}
			insertRowWithData(data);
		});
	}
}

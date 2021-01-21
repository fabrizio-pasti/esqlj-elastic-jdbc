package org.takeshi.jdbc.esqlj.elastic.query.impl;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.takeshi.jdbc.esqlj.EsConnection;
import org.takeshi.jdbc.esqlj.elastic.query.AbstractOneShotQuery;

public class FromArrayQuery extends AbstractOneShotQuery {
			
	public FromArrayQuery(EsConnection connection, String source, List<List<Object>> values, String... columnsName) throws SQLException {
		super(connection, source, columnsName);
		init(values);
	}

	public void init(List<List<Object>> values) throws SQLException {
		values.forEach(rowData -> {
			Map<String, Object> data = new HashMap<String, Object>();
			for(int idx = 0; idx < this.getColumnsName().size(); idx++) {
				data.put(this.getColumnsName().get(idx), rowData.get(idx));
			}
			insertRowWithData(data);
		});
	}
}

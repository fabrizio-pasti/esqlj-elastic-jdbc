package org.takeshi.jdbc.esqlj.elastic.query;

import java.sql.Connection;
import java.sql.ResultSet;

import org.takeshi.jdbc.esqlj.elastic.query.impl.ScrollableQuery;

public class Executor {
	
	public static ResultSet execSql(Connection connecton, String sql) {
		return null;
		//return new ScrollableQuery(null, sql);
	}
}

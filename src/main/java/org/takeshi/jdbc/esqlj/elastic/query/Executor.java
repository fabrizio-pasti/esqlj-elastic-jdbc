package org.takeshi.jdbc.esqlj.elastic.query;

import java.sql.SQLException;

import org.takeshi.jdbc.esqlj.EsConnection;
import org.takeshi.jdbc.esqlj.elastic.query.impl.ElasticQuery;
import org.takeshi.jdbc.esqlj.parser.model.Field;
import org.takeshi.jdbc.esqlj.parser.model.Index;
import org.takeshi.jdbc.esqlj.parser.model.ParsedQuery;

public class Executor {
	
	public static ElasticQuery execSql(EsConnection connection, String sql) throws SQLException {
		return new ElasticQuery(connection, tempQuery(), true);
	}
	
	private static ParsedQuery tempQuery( ) {
		ParsedQuery q = new ParsedQuery();
		Index i = new Index();
		i.setName("monitoring_segment_tx_20201");
		q.setIndex(i);
		Field f1 = new Field("idSession", "alias1", null);
		Field f2 = new Field("amount", "alias2", null);
		q.getFields().add(f1);
		q.getFields().add(f2);
		return q; 
	}
}

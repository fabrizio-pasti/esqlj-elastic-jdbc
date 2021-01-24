package org.takeshi.jdbc.esqlj.elastic.query;

import java.sql.SQLException;

import org.takeshi.jdbc.esqlj.EsConnection;
import org.takeshi.jdbc.esqlj.elastic.query.impl.ScrollableQuery;
import org.takeshi.jdbc.esqlj.parser.model.Field;
import org.takeshi.jdbc.esqlj.parser.model.Index;
import org.takeshi.jdbc.esqlj.parser.model.ParsedQuery;

public class Executor {
	
	public static ScrollableQuery execSql(EsConnection connection, String sql) throws SQLException {
		return new ScrollableQuery(connection, tempQuery());
	}
	
	private static ParsedQuery tempQuery( ) {
		ParsedQuery q = new ParsedQuery();
		Index i = new Index();
		i.setName("monitoring_segment_tx_20201");
		q.setIndex(i);
		Field f1 = new Field("idSession", null, null);
		Field f2 = new Field("amount", null, null);
		q.getFields().add(f1);
		q.getFields().add(f2);
		return q;
	}
}

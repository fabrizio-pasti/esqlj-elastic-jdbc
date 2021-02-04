package org.takeshi.jdbc.esqlj.elastic.query.impl.search;

import org.elasticsearch.search.sort.SortOrder;
import org.takeshi.jdbc.esqlj.elastic.query.statement.SqlStatementSelect;

import net.sf.jsqlparser.schema.Column;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class RequestBuilderSort {
	
	public static void manageSort(SqlStatementSelect select, RequestInstance req) {
		select.getOrderByFields().stream().forEach(elem -> {
			req.getSearchSourceBuilder().sort(((Column)elem.getExpression()).getColumnName(), elem.isAsc() ? SortOrder.ASC : SortOrder.DESC);
		});
	}

}

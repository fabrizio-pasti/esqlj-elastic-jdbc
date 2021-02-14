package org.fpasti.jdbc.esqlj.elastic.query.impl.search.clause.select;

import java.sql.SQLSyntaxErrorException;

import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.fpasti.jdbc.esqlj.elastic.query.impl.search.RequestInstance;
import org.fpasti.jdbc.esqlj.elastic.query.statement.SqlStatementSelect;

import net.sf.jsqlparser.schema.Column;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class AggregationBuilder {

	@SuppressWarnings("incomplete-switch")
	public static void doAggregation(SqlStatementSelect select, RequestInstance req) throws SQLSyntaxErrorException {
		switch(select.getQueryType()) {
			case AGGR_COUNT_ALL:
				manageCountAllExpression(req);
				break;
			case AGGR_COUNT_FIELD:
				manageCountFieldExpression(select, req);
				break;
			case AGGR_GROUPED:
				manageGroupedExpression(select, req);
				break;			
		}
	}
	
	private static void manageCountAllExpression(RequestInstance req) {
		req.getSearchSourceBuilder().size(0);
	}
	
	private static void manageCountFieldExpression(SqlStatementSelect select, RequestInstance req) {
		Column countField = (Column)select.getFields().get(0).getAggregatingFunction().getParameters().getExpressions().get(0);
		req.getSearchSourceBuilder().aggregation(AggregationBuilders.count(countField.getColumnName()));
	}

	private static void manageGroupedExpression(SqlStatementSelect select, RequestInstance req) {
		// not yet implemented
	}
	
}

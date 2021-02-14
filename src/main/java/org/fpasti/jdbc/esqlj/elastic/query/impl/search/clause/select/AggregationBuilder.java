package org.fpasti.jdbc.esqlj.elastic.query.impl.search.clause.select;

import java.sql.SQLSyntaxErrorException;

import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.fpasti.jdbc.esqlj.elastic.query.impl.search.RequestInstance;
import org.fpasti.jdbc.esqlj.elastic.query.statement.SqlStatementSelect;
import org.fpasti.jdbc.esqlj.elastic.query.statement.model.QueryColumn;

import net.sf.jsqlparser.schema.Column;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class AggregationBuilder {

	@SuppressWarnings("incomplete-switch")
	public static void doAggregation(SqlStatementSelect select, RequestInstance req) throws SQLSyntaxErrorException {
		switch(select.getQueryType()) {
			case AGGR_COUNT_FIELD:
				select.getQueryColumns().forEach(queryColumn -> manageCountFieldExpression(queryColumn, req));
				break;
			case AGGR_GROUPED:
				manageGroupedExpression(select, req);
				break;			
		}
		req.getSearchSourceBuilder().size(0);
	}
		
	private static void manageCountFieldExpression(QueryColumn queryColumn, RequestInstance req) {
		Column countField = (Column)queryColumn.getAggregatingFunction().getParameters().getExpressions().get(0);
		req.getSearchSourceBuilder().aggregation(AggregationBuilders.count(queryColumn.getAggregatingColumnName()).field(countField.getColumnName().replace("\"", "")));
	}

	private static void manageGroupedExpression(SqlStatementSelect select, RequestInstance req) {
		// not yet implemented
	}
	
}

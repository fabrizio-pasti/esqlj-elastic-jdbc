package org.fpasti.jdbc.esqlj.elastic.query.impl.search.clause.select;

import java.sql.SQLSyntaxErrorException;

import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.fpasti.jdbc.esqlj.Configuration;
import org.fpasti.jdbc.esqlj.ConfigurationPropertyEnum;
import org.fpasti.jdbc.esqlj.elastic.query.impl.search.RequestInstance;
import org.fpasti.jdbc.esqlj.elastic.query.impl.search.clause.ClauseHaving;
import org.fpasti.jdbc.esqlj.elastic.query.statement.SqlStatementSelect;
import org.fpasti.jdbc.esqlj.elastic.query.statement.model.FunctionEnum;
import org.fpasti.jdbc.esqlj.elastic.query.statement.model.QueryColumn;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class AggregationBuilder {

	@SuppressWarnings("incomplete-switch")
	public static void doAggregation(RequestInstance req) throws SQLSyntaxErrorException {
		switch(req.getSelect().getQueryType()) {
			case AGGR_UNGROUPED_EXPRESSIONS:
				manageUngroupedExpression(req);
				break;
			case AGGR_GROUP_BY:
				manageGroupByExpressions(req);
				break;	
			case DISTINCT_DOCS:
				manageDistinctColumns(req);
				break;
		}
		req.getSearchSourceBuilder().size(0);
	}

	private static void manageUngroupedExpression(RequestInstance req) throws SQLSyntaxErrorException {
		SearchSourceBuilder builder = req.getSearchSourceBuilder();
		for(Integer idx = 0; idx < req.getSelect().getQueryColumns().size(); idx++) {
			QueryColumn queryColumn = req.getSelect().getQueryColumns().get(idx);
			addGroupingFunction(queryColumn, idx.toString(), req.getSelect(), null, builder);
		}
	}

	public static void manageGroupByExpressions(RequestInstance req) throws SQLSyntaxErrorException {
		SearchSourceBuilder builder = req.getSearchSourceBuilder();
		TermsAggregationBuilder currentTermsAggregationBuilder = null;
		
		for(String column : req.getSelect().getGroupByColumns()) {
			TermsAggregationBuilder tab = AggregationBuilders.terms(getPositionInClauseSelect(req.getSelect(), column)).field(column).size(Configuration.getConfiguration(ConfigurationPropertyEnum.CFG_MAX_GROUP_BY_RETRIEVED_ELEMENTS, Integer.class));
			
			manageOrdering(column, tab, req.getSelect());
			
			if(currentTermsAggregationBuilder == null) {
				builder.aggregation(tab);
			} else {
				currentTermsAggregationBuilder.subAggregation(tab);
			}
			currentTermsAggregationBuilder = tab;
		}
		
		final TermsAggregationBuilder deeperTermsAggregationBuilder = currentTermsAggregationBuilder;
	
		for(Integer i = 0; i < req.getSelect().getQueryColumns().size(); i++) {
			QueryColumn column = req.getSelect().getQueryColumns().get(i);
			if(column.getAggregatingFunctionExpression() != null) {
				addGroupingFunction(column, i.toString(), req.getSelect(), deeperTermsAggregationBuilder, null);
			}
		}
		
		if(req.getSelect().getHavingCondition() != null) {
			ClauseHaving.manageHavingCondition(req.getSelect(), deeperTermsAggregationBuilder);
		}
	}
	
	private static void manageDistinctColumns(RequestInstance req) {
		SearchSourceBuilder builder = req.getSearchSourceBuilder();
		TermsAggregationBuilder currentTermsAggregationBuilder = null;
		
		for(QueryColumn queryColumn : req.getSelect().getQueryColumns()) {
			TermsAggregationBuilder tab = AggregationBuilders.terms(getPositionInClauseSelect(req.getSelect(), queryColumn.getName())).field(queryColumn.getName()).size(Configuration.getConfiguration(ConfigurationPropertyEnum.CFG_MAX_GROUP_BY_RETRIEVED_ELEMENTS, Integer.class));
			
			manageOrdering(queryColumn.getName(), tab, req.getSelect());
			
			if(currentTermsAggregationBuilder == null) {
				builder.aggregation(tab);
			} else {
				currentTermsAggregationBuilder.subAggregation(tab);
			}
			currentTermsAggregationBuilder = tab;
		}
	}

	private static void manageOrdering(String columnName, TermsAggregationBuilder termsAggregation, SqlStatementSelect select) {
		OrderByElement orderByElement = select.getOrderByElements().stream().filter(orderBy -> ((Column)orderBy.getExpression()).getColumnName().equalsIgnoreCase(columnName)).findFirst().orElse(null);
		if(orderByElement != null) {
			termsAggregation.order(BucketOrder.key(orderByElement.isAsc()));
		}
	}

	private static void addGroupingFunction(QueryColumn column, String columnPosition, SqlStatementSelect select, TermsAggregationBuilder termsAggregation, SearchSourceBuilder builder) throws SQLSyntaxErrorException {
		org.elasticsearch.search.aggregations.AggregationBuilder aggregationBuilder = null;
		
		switch(column.getFunctionType()) {
			case AVG:
				aggregationBuilder = AggregationBuilders.avg(columnPosition).field(stripDoubleQuotes(column.getAggregatingFunctionExpression().getParameters().getExpressions().get(0).toString()));
				break;
			case COUNT:
				if(column.getAggregatingFunctionExpression().isAllColumns()) {
					aggregationBuilder = AggregationBuilders.count(columnPosition).script(new Script("1"));
				} else if(column.getAggregatingFunctionExpression().isDistinct()) {
					aggregationBuilder = AggregationBuilders.cardinality(columnPosition).field(stripDoubleQuotes(column.getAggregatingFunctionExpression().getParameters().getExpressions().get(0).toString()));
				} else {
					aggregationBuilder = AggregationBuilders.count(columnPosition).field(stripDoubleQuotes(column.getAggregatingFunctionExpression().getParameters().getExpressions().get(0).toString()));
				}
				break;
			case MAX:
				aggregationBuilder = AggregationBuilders.max(columnPosition).field(stripDoubleQuotes(column.getAggregatingFunctionExpression().getParameters().getExpressions().get(0).toString()));
				break;
			case MIN:
				aggregationBuilder = AggregationBuilders.min(columnPosition).field(stripDoubleQuotes(column.getAggregatingFunctionExpression().getParameters().getExpressions().get(0).toString()));
				break;
			case SUM:
				aggregationBuilder = AggregationBuilders.sum(columnPosition).field(stripDoubleQuotes(column.getAggregatingFunctionExpression().getParameters().getExpressions().get(0).toString()));
				break;
			default:
				throw new SQLSyntaxErrorException(String.format("Expression %s unsupported", column.getAggregatingFunctionExpression().getName()));
		}
		
		if(aggregationBuilder != null) {
			if(termsAggregation != null) {
				termsAggregation.subAggregation(aggregationBuilder);
				
				OrderByElement orderByElement = select.getOrderByElements().stream().filter(orderBy -> ((Column)orderBy.getExpression()).getColumnName().equalsIgnoreCase(column.getName()) || ((Column)orderBy.getExpression()).getColumnName().equalsIgnoreCase(column.getAlias())).findFirst().orElse(null);
				if(orderByElement != null) {
					termsAggregation.order(BucketOrder.aggregation(aggregationBuilder.getName(), orderByElement.isAsc()));
				}
			} else {
				builder.aggregation(aggregationBuilder);
			}
		}
	}

	
	
	private static String stripDoubleQuotes(String str) {
		return str.replaceAll("\"", "");
	}

	private static String getPositionInClauseSelect(SqlStatementSelect select, String columnName) {
		for(Integer i = 0; i < select.getQueryColumns().size(); i++) {
			if(select.getQueryColumns().get(i).getName().equalsIgnoreCase(columnName)) {
				return i.toString(); 
			}
		}
		return columnName;
	}
}

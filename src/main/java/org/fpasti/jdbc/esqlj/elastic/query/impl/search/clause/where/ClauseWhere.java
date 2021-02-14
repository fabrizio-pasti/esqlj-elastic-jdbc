	package org.fpasti.jdbc.esqlj.elastic.query.impl.search.clause.where;


import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.fpasti.jdbc.esqlj.elastic.query.impl.search.RequestInstance;
import org.fpasti.jdbc.esqlj.elastic.query.impl.search.model.ElasticScriptMethodEnum;
import org.fpasti.jdbc.esqlj.elastic.query.impl.search.model.EvaluateQueryResult;
import org.fpasti.jdbc.esqlj.elastic.query.impl.search.model.TermsQuery;
import org.fpasti.jdbc.esqlj.elastic.query.statement.SqlStatementSelect;
import org.fpasti.jdbc.esqlj.elastic.query.statement.model.ExpressionEnum;
import org.fpasti.jdbc.esqlj.elastic.query.statement.model.QueryColumn;

import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class ClauseWhere {
	
	public static void manageWhere(SqlStatementSelect select, RequestInstance req) throws SQLException {
		if(select.getWhereCondition() == null) {
			return;
		}
		
		EvaluateQueryResult result = evaluateQueryExpression(select.getWhereCondition(), select);
		
		QueryBuilder qb = null;
		
		switch(result.getType()) {
			case ONLY_ONE:
				qb = result.getQueryBuilders().get(0);
				break;
			case ONLY_ONE_NOT:
				qb = result.getNotQueryBuilders().get(0);
				break;
			case ONLY_ONE_TERMS:
				Map.Entry<String,List<Object>> terms = result.getTermsQuery().getEqualObjects().entrySet().iterator().next();
				if(terms.getValue().size() == 1) {
					qb = QueryBuilders.termQuery(terms.getKey(), terms.getValue().get(0));
				} else {
					qb = QueryBuilders.termsQuery(terms.getKey(), terms.getValue());
				}
				break;
			case ONLY_ONE_NOT_TERMS:
				Map.Entry<String,List<Object>> notTerms = result.getTermsQuery().getNotEqualObjects().entrySet().iterator().next();
				qb = QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery(notTerms.getKey(), notTerms.getValue().get(0)));
				break;
			case MIXED:
				qb = QueryBuilders.boolQuery();
				getQueryBuilderFromResult(select.getWhereCondition(), result, qb);
				break;
		}
		
		req.getSearchSourceBuilder().query(qb);
	}
	
	@SuppressWarnings("unchecked")
	private static EvaluateQueryResult evaluateQueryExpression(Expression expression, SqlStatementSelect select) throws SQLException {
		switch(ExpressionEnum.resolveByInstance(expression)) {
			case AND_EXPRESSION:
				AndExpression andExpression = (AndExpression)expression;
				EvaluateQueryResult resAndLeft = evaluateQueryExpression(andExpression.getLeftExpression(), select);
				EvaluateQueryResult resAndRight = evaluateQueryExpression(andExpression.getRightExpression(), select);
				resAndLeft.merge(true, resAndRight);
				return resAndLeft;
			case OR_EXPRESSION:
				OrExpression orExpression = (OrExpression)expression;
				BoolQueryBuilder leftBoolQueryBuilder = null;
				BoolQueryBuilder rightBoolQueryBuilder = null;

				EvaluateQueryResult resOrLeft = evaluateQueryExpression(orExpression.getLeftExpression(), select);
				EvaluateQueryResult resOrRight = evaluateQueryExpression(orExpression.getRightExpression(), select);
				
				if(ExpressionEnum.isInstanceOf(orExpression.getLeftExpression(), ExpressionEnum.AND_EXPRESSION) 
						|| ExpressionEnum.isInstanceOf(orExpression.getLeftExpression(), ExpressionEnum.PARENTHESIS)
						|| ExpressionEnum.isInstanceOf(orExpression.getLeftExpression(), ExpressionEnum.NOT_EXPRESSION)) {
					leftBoolQueryBuilder = createBoolQueryBuilder(resOrLeft);
				}
				
				if(ExpressionEnum.isInstanceOf(orExpression.getRightExpression(), ExpressionEnum.AND_EXPRESSION) 
						|| ExpressionEnum.isInstanceOf(orExpression.getRightExpression(), ExpressionEnum.PARENTHESIS) 
						|| ExpressionEnum.isInstanceOf(orExpression.getRightExpression(), ExpressionEnum.NOT_EXPRESSION)) {
					rightBoolQueryBuilder = createBoolQueryBuilder(resOrRight);
				}
				
				QueryBuilder qbOr = null;
				if(leftBoolQueryBuilder != null && rightBoolQueryBuilder == null) {
					qbOr = mapResultToQueryBuilder(leftBoolQueryBuilder, resOrRight);
				} else if(rightBoolQueryBuilder != null && leftBoolQueryBuilder == null) {
					qbOr = mapResultToQueryBuilder(rightBoolQueryBuilder, resOrLeft);
				} else if(leftBoolQueryBuilder != null && rightBoolQueryBuilder != null) {
					qbOr = QueryBuilders.boolQuery();
					((BoolQueryBuilder)qbOr).should().add(rightBoolQueryBuilder);
					((BoolQueryBuilder)qbOr).should().add(leftBoolQueryBuilder);
				} 
				
				if(qbOr != null) {
					return new EvaluateQueryResult(qbOr);
				}
				
				return resOrLeft.merge(false, resOrRight);
			case PARENTHESIS:
				Parenthesis parenthesis = (Parenthesis)expression;
				return evaluateQueryExpression(parenthesis.getExpression(), select);
			case GREATER_THAN:
				GreaterThan greaterThan = (GreaterThan)expression;
				if(greaterThan.getLeftExpression() instanceof ExtractExpression) {
					return resolveExtract(greaterThan.getLeftExpression(), greaterThan.getRightExpression(), ">", select);
				}
				return new EvaluateQueryResult(QueryBuilders.rangeQuery(getColumn(greaterThan.getLeftExpression(), select)).gt(ExpressionResolverValue.evaluateValueExpression(greaterThan.getRightExpression())));
			case GREATER_THAN_EQUALS:
				GreaterThanEquals greaterThanEquals = (GreaterThanEquals)expression;
				if(greaterThanEquals.getLeftExpression() instanceof ExtractExpression) {
					return resolveExtract(greaterThanEquals.getLeftExpression(), greaterThanEquals.getRightExpression(), ">=", select);
				}
				return new EvaluateQueryResult(QueryBuilders.rangeQuery(getColumn(greaterThanEquals.getLeftExpression(), select)).gte(ExpressionResolverValue.evaluateValueExpression(greaterThanEquals.getRightExpression())));
			case MINOR_THAN:
				MinorThan minorThan = (MinorThan)expression;
				if(minorThan.getLeftExpression() instanceof ExtractExpression) {
					return resolveExtract(minorThan.getLeftExpression(), minorThan.getRightExpression(), "<", select);
				}
				return new EvaluateQueryResult(QueryBuilders.rangeQuery(getColumn(minorThan.getLeftExpression(), select)).lt(ExpressionResolverValue.evaluateValueExpression(minorThan.getRightExpression())));
			case MINOR_THAN_EQUALS:
				MinorThanEquals minorThanEquals = (MinorThanEquals)expression;
				if(minorThanEquals.getLeftExpression() instanceof ExtractExpression) {
					return resolveExtract(minorThanEquals.getLeftExpression(), minorThanEquals.getRightExpression(), "<=", select);
				}
				return new EvaluateQueryResult(QueryBuilders.rangeQuery(getColumn(minorThanEquals.getLeftExpression(), select)).lte(ExpressionResolverValue.evaluateValueExpression(minorThanEquals.getRightExpression())));
			case EQUALS_TO:
				EqualsTo equalsTo = (EqualsTo)expression;
				if(equalsTo.getLeftExpression() instanceof ExtractExpression) {
					return resolveExtract(equalsTo.getLeftExpression(), equalsTo.getRightExpression(), "==", select);
				}
				EvaluateQueryResult etQr = new EvaluateQueryResult();
				etQr.addEqualTerm(getColumn(equalsTo.getLeftExpression(), select), ExpressionResolverValue.evaluateValueExpression(equalsTo.getRightExpression()));
				return etQr;
			case NOT_EQUALS_TO:
				NotEqualsTo notEqualsTo = (NotEqualsTo)expression;
				if(notEqualsTo.getLeftExpression() instanceof ExtractExpression) {
					return resolveExtract(notEqualsTo.getLeftExpression(), notEqualsTo.getRightExpression(), "!=", select);
				}
				EvaluateQueryResult netQr = new EvaluateQueryResult();
				netQr.addNotEqualTerm(getColumn(notEqualsTo.getLeftExpression(), select), ExpressionResolverValue.evaluateValueExpression(notEqualsTo.getRightExpression()));
				return netQr;
			case IS_NULL_EXPRESSION:
 				IsNullExpression isNullExpression = (IsNullExpression)expression;
 				if(isNullExpression.isNot()) {
 					return new EvaluateQueryResult(QueryBuilders.existsQuery(getColumn(isNullExpression.getLeftExpression(), select)));
 				}
				BoolQueryBuilder qbEqrIneNot = QueryBuilders.boolQuery();
				qbEqrIneNot.mustNot().add(QueryBuilders.existsQuery(getColumn(isNullExpression.getLeftExpression(), select)));
				EvaluateQueryResult eqrIne = new EvaluateQueryResult(qbEqrIneNot);
				eqrIne.setReverseNegateOnNot(true);
				return eqrIne;
			case NOT_EXPRESSION:
				NotExpression notExpression = (NotExpression)expression;
				EvaluateQueryResult res = evaluateQueryExpression(notExpression.getExpression(), select);
				BoolQueryBuilder qbNot = QueryBuilders.boolQuery();
				getQueryBuilderFromResult(notExpression.getExpression(), res, qbNot);
				BoolQueryBuilder qb = QueryBuilders.boolQuery();
				qb.mustNot().add(qbNot);
				return new EvaluateQueryResult(qb);
			case BETWEEN:
				Between between = (Between)expression;
				if(between.getLeftExpression() instanceof ExtractExpression) {
					return resolveExtractBetween(between, select);
				}
				return new EvaluateQueryResult(QueryBuilders.rangeQuery(getColumn(between.getLeftExpression(), select)).gte(ExpressionResolverValue.evaluateValueExpression(between.getBetweenExpressionStart())).lte(ExpressionResolverValue.evaluateValueExpression(between.getBetweenExpressionEnd())));
			case LIKE_EXPRESSION:
				LikeExpression likeExpression = (LikeExpression)expression;
				return new EvaluateQueryResult(QueryBuilders.wildcardQuery(getColumn(likeExpression.getLeftExpression(), select), (String)ExpressionResolverValue.evaluateValueExpression(likeExpression.getRightExpression())));
			case IN_EXPRESSION:
				InExpression inExpression = (InExpression)expression;
				if(inExpression.getLeftExpression() instanceof ExtractExpression) {
					return resolveExtract(inExpression.getLeftExpression(), inExpression.getRightItemsList(), "==", select);
				}
				EvaluateQueryResult etQrIe = new EvaluateQueryResult();
				etQrIe.addEqualTerms(getColumn(inExpression.getLeftExpression(), select), (List<Object>)ExpressionResolverValue.evaluateValueExpression(inExpression.getRightItemsList()));
				return etQrIe;
			case CAST_EXPRESSION:
				CastExpression castExpression = (CastExpression)expression;
				if(!(castExpression.getLeftExpression() instanceof Column) || !((Column)castExpression.getLeftExpression()).getColumnName().equalsIgnoreCase("_elAPI")) {
					throw new SQLException("[::] syntax must to be used only with '_elAPI'");
				}
				return ExpressionResolverElApi.manageExpression(castExpression.getType());
			default:
				throw new SQLException(String.format("Unmanaged expression: %s", ExpressionEnum.resolveByInstance(expression).name()));
		}
	}

	private static EvaluateQueryResult resolveExtract(Expression extractExpression, Object valueExpression, String operator, SqlStatementSelect select) throws SQLException {
		ExtractExpression extract = (ExtractExpression)extractExpression;
		ElasticScriptMethodEnum scriptDateMethod = null;
		try {
			scriptDateMethod = ElasticScriptMethodEnum.valueOf(extract.getName());
		} catch(IllegalArgumentException e) {
			throw new SQLSyntaxErrorException(String.format("Unsupported extract params '%s'", extract.getName()));
		}

		if(ExpressionEnum.resolveByInstance(valueExpression) == ExpressionEnum.EXPRESSION_LIST) {
			ExpressionList expressionList = (ExpressionList)valueExpression;
			Map<String, Object> params = new HashMap<String, Object>();
			String scriptExpression = "";
			for(int i = 0; i < expressionList.getExpressions().size(); i++) {
				Expression expression = expressionList.getExpressions().get(i);
				params.put(String.format("param%d", i), ExpressionResolverValue.evaluateValueExpression(expression));
				scriptExpression = scriptExpression.concat(scriptExpression.length() == 0 ? "" : " || ").concat(String.format("doc.%s.value.%s %s params.param%d", getColumn(extract.getExpression(), select), scriptDateMethod.getMethod(), operator, i));
			}
			Script script = new Script(Script.DEFAULT_SCRIPT_TYPE, Script.DEFAULT_SCRIPT_LANG, scriptExpression, params);
			return new EvaluateQueryResult(QueryBuilders.scriptQuery(script));
		}
		
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("param", ExpressionResolverValue.evaluateValueExpression(valueExpression));
		Script script = new Script(Script.DEFAULT_SCRIPT_TYPE, Script.DEFAULT_SCRIPT_LANG, String.format("doc.%s.value.%s %s params.param", getColumn(extract.getExpression(), select), scriptDateMethod.getMethod(), operator), params);
		return new EvaluateQueryResult(QueryBuilders.scriptQuery(script));
	}
	
	private static EvaluateQueryResult resolveExtractBetween(Between between, SqlStatementSelect select) throws SQLException {
		ExtractExpression extract = (ExtractExpression)between.getLeftExpression();
		ElasticScriptMethodEnum scriptDateMethod = null;
		try {
			scriptDateMethod = ElasticScriptMethodEnum.valueOf(extract.getName());
		} catch(IllegalArgumentException e) {
			throw new SQLSyntaxErrorException(String.format("Unsupported extract params '%s'", extract.getName()));
		}
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("param1", ExpressionResolverValue.evaluateValueExpression(between.getBetweenExpressionStart()));
		params.put("param2", ExpressionResolverValue.evaluateValueExpression(between.getBetweenExpressionEnd()));
		Script script = new Script(Script.DEFAULT_SCRIPT_TYPE, Script.DEFAULT_SCRIPT_LANG, String.format("doc.%s.value.%s >= params.param1 && doc.%s.value.%s <= params.param2", getColumn(extract.getExpression(), select), scriptDateMethod.getMethod(), getColumn(extract.getExpression(), select), scriptDateMethod.getMethod()), params);
		return new EvaluateQueryResult(QueryBuilders.scriptQuery(script));
	}

	private static void getQueryBuilderFromResult(Expression expression, EvaluateQueryResult result,
			QueryBuilder qb) {
		if(!result.isListEmpty()) {
			if(result.isAnd()) {
				((BoolQueryBuilder)qb).must().addAll(result.getQueryBuilders());
			} else {
				((BoolQueryBuilder)qb).should().addAll(result.getQueryBuilders());
			}
		}
		if(!result.isNotListEmpty()) {
			((BoolQueryBuilder)qb).mustNot().addAll(result.getNotQueryBuilders());
		}
		if(!result.isTermsEmpty()) {
			addTermsQuery((BoolQueryBuilder)qb, result.getTermsQuery(), result.isAnd());
		}
	}
	
	private static BoolQueryBuilder createBoolQueryBuilder(EvaluateQueryResult queryResult) {
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		boolQueryBuilder.must().addAll(queryResult.getQueryBuilders());
		boolQueryBuilder.mustNot().addAll(queryResult.getNotQueryBuilders());
		addTermsQuery(boolQueryBuilder, queryResult.getTermsQuery(), true);
		return boolQueryBuilder;
	}
	
	private static QueryBuilder mapResultToQueryBuilder(QueryBuilder queryBuilder, EvaluateQueryResult result) {
		BoolQueryBuilder qbOr = QueryBuilders.boolQuery();

		qbOr.should().add(queryBuilder);
		qbOr.should().addAll(result.getQueryBuilders());
		addTermsQuery(qbOr, result.getTermsQuery(), false);
		if(result.getNotQueryBuilders().size() > 0) {
			BoolQueryBuilder mustNot = QueryBuilders.boolQuery();
			mustNot.mustNot().addAll(result.getNotQueryBuilders());
			qbOr.should().add(mustNot);
		}
		return qbOr;
	}

	private static void addTermsQuery(BoolQueryBuilder qb, TermsQuery queryContents, boolean and) {
		if(queryContents.getEqualObjects() != null && !queryContents.getEqualObjects().isEmpty()) {
			queryContents.getEqualObjects().forEach((field, values) -> {
				if(and) {
					values.stream().forEach(value -> qb.must().add(QueryBuilders.termQuery(field, value)));
				}
				else {
					qb.should().add(QueryBuilders.termsQuery(field, values));
				} 
			});
		}
		
		if(queryContents.getNotEqualObjects() != null && !queryContents.getNotEqualObjects().isEmpty()) {
			BoolQueryBuilder mustNotQb = QueryBuilders.boolQuery();
			
			queryContents.getNotEqualObjects().forEach((field, values) -> {
				if(and) {
					values.stream().forEach(value -> qb.mustNot().add(QueryBuilders.termQuery(field, value)));
				} else {
					mustNotQb.mustNot().add(QueryBuilders.termsQuery(field, values));
				} 
			});
			
			if(!and) {
				qb.should().add(mustNotQb);
			}
		}
	}
	
	private static String getColumn(Expression expression, SqlStatementSelect select) throws SQLException {
		if(!(expression instanceof Column)) {
			throw new SQLException(String.format("Not Column expression: %s", ExpressionEnum.resolveByInstance(expression).name()));
		}
		
		Column column = (Column)expression;
		String columnName = column.getColumnName().replace("\"", "");
		QueryColumn columnField = select.getColumnsByNameOrAlias(columnName);
		return columnField != null ? columnField.getName() : columnName;
	}
	
}

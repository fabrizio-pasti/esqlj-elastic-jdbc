package org.takeshi.jdbc.esqlj.elastic.query.impl.search;


import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.takeshi.jdbc.esqlj.elastic.query.statement.SqlStatementSelect;
import org.takeshi.jdbc.esqlj.elastic.query.statement.model.ExpressionEnum;
import org.takeshi.jdbc.esqlj.support.ToDateUtils;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;

public class RequestBuilderWhere {
	
	private static class QueryContents {
		private Map<String, List<Object>> equalObjects;
		private Map<String, List<Object>> notEqualObjects;
		
		public void addEqualObject(String field, Object eqO) {
			if(equalObjects == null) {
				equalObjects = new HashMap<String, List<Object>>();
			}
			if(!equalObjects.containsKey(field)) {
				equalObjects.put(field, new ArrayList<Object>());
			}
			equalObjects.get(field).add(eqO);
		}

		public void addNotEqualObject(String field, Object nEqO) {
			if(notEqualObjects == null) {
				notEqualObjects = new HashMap<String, List<Object>>();
			}
			if(!notEqualObjects.containsKey(field)) {
				notEqualObjects.put(field, new ArrayList<Object>());
			}
			notEqualObjects.get(field).add(nEqO);
		}

		public Map<String, List<Object>> getEqualObjects() {
			return equalObjects;
		}
		
		public Map<String, List<Object>> getNotEqualObjects() {
			return notEqualObjects;
		}
		
	}
	
	public static void manageWhere(SqlStatementSelect select, RequestInstance req) throws SQLException {
		if(select.getWhereCondition() == null) {
			return;
		}
		
		req.getSearchSourceBuilder().query(evaluateQueryExpression(select.getWhereCondition(), select, null));
	}
	
	private static QueryBuilder evaluateQueryExpression(Expression expression, SqlStatementSelect select, QueryContents queryContents) throws SQLException {
		ExpressionEnum expressionType = ExpressionEnum.resolveByInstance(expression);
		
		switch(ExpressionEnum.resolveByInstance(expression)) {
			case AND_EXPRESSION:
				AndExpression andExpression = (AndExpression)expression;
				BoolQueryBuilder qbAnd = QueryBuilders.boolQuery();
				QueryBuilder retAndLeft = evaluateQueryExpression(andExpression.getLeftExpression(), select, null);
				QueryBuilder retAndRight = evaluateQueryExpression(andExpression.getRightExpression(), select, null);
				if(ExpressionEnum.resolveByInstance(andExpression.getLeftExpression()) == ExpressionEnum.NOT_EQUALS_TO) {
					qbAnd.mustNot(retAndLeft);
				} else {
					qbAnd.must().add(retAndLeft);
				}
				
				if(ExpressionEnum.resolveByInstance(andExpression.getRightExpression()) == ExpressionEnum.NOT_EQUALS_TO) {
					qbAnd.mustNot(retAndRight);
				} else {
					qbAnd.must().add(retAndRight);
				}
				return qbAnd;
			case OR_EXPRESSION:
				OrExpression orExpression = (OrExpression)expression;
				BoolQueryBuilder qbOr = QueryBuilders.boolQuery();
				
				boolean deepTerms = false;
				if(queryContents == null) {
					queryContents = new QueryContents();
					deepTerms = true;
				}

				QueryBuilder retOr = evaluateQueryExpression(orExpression.getRightExpression(), select, queryContents);
				if(retOr == null) {
					retOr = evaluateQueryExpression(orExpression.getLeftExpression(), select, queryContents);
					if(retOr == null) {
						addTermsQuery(qbOr, queryContents);
						return qbOr;
					} else {
						qbOr.should().add(retOr);
						return retOr;
					}
				} else {
					qbOr.should().add(evaluateQueryExpression(orExpression.getLeftExpression(), select, null));
					qbOr.should().add(retOr);					
				}
				
				if(deepTerms) {
					addTermsQuery(qbOr, queryContents);
				}
				return qbOr;
			case PARENTHESIS:
				Parenthesis parenthesis = (Parenthesis)expression;
				return evaluateQueryExpression(parenthesis.getExpression(), select, null);
			case GREATER_THAN:
				GreaterThan greaterThan = (GreaterThan)expression;
				return QueryBuilders.rangeQuery(getColumn(greaterThan.getLeftExpression())).gt(evaluateValueExpression(greaterThan.getRightExpression()));
			case GREATER_THAN_EQUALS:
				GreaterThanEquals greaterThanEquals = (GreaterThanEquals)expression;
				return QueryBuilders.rangeQuery(getColumn(greaterThanEquals.getLeftExpression())).gte(evaluateValueExpression(greaterThanEquals.getRightExpression()));
			case MINOR_THAN:
				MinorThan minorThan = (MinorThan)expression;
				return QueryBuilders.rangeQuery(getColumn(minorThan.getLeftExpression())).lt(evaluateValueExpression(minorThan.getRightExpression()));
			case MINOR_THAN_EQUALS:
				MinorThanEquals minorThanEquals = (MinorThanEquals)expression;
				return QueryBuilders.rangeQuery(getColumn(minorThanEquals.getLeftExpression())).lte(evaluateValueExpression(minorThanEquals.getRightExpression()));
			case EQUALS_TO:
				EqualsTo equalsTo = (EqualsTo)expression;
				if(queryContents == null) {
					return QueryBuilders.termsQuery(getColumn(equalsTo.getLeftExpression()), evaluateValueExpression(equalsTo.getRightExpression()));
				} else {
					queryContents.addEqualObject(getColumn(equalsTo.getLeftExpression()), evaluateValueExpression(equalsTo.getRightExpression()));
					return null;
				}
			case NOT_EQUALS_TO:
				NotEqualsTo notEqualsTo = (NotEqualsTo)expression;
				if(queryContents == null) {
					return QueryBuilders.termQuery(getColumn(notEqualsTo.getLeftExpression()), evaluateValueExpression(notEqualsTo.getRightExpression()));
				} else {
					queryContents.addNotEqualObject(getColumn(notEqualsTo.getLeftExpression()), evaluateValueExpression(notEqualsTo.getRightExpression()));
					return null;
				}
			case IS_NULL_EXPRESSION:
 				IsNullExpression isNullExpression = (IsNullExpression)expression;
 				QueryBuilder isNullQueryBuilder = QueryBuilders.existsQuery(getColumn(isNullExpression.getLeftExpression()));
 				if(isNullExpression.isNot()) {
 					BoolQueryBuilder mustNot = QueryBuilders.boolQuery();
 					mustNot.mustNot().add(isNullQueryBuilder);
 					return mustNot;
 				}
				return isNullQueryBuilder;
			case NOT_EXPRESSION:
				NotExpression notExpression = (NotExpression)expression;
				BoolQueryBuilder mustNot = QueryBuilders.boolQuery();
				mustNot.mustNot().add(evaluateQueryExpression(notExpression.getExpression(), select, queryContents));
				return mustNot;
			default:
				throw new SQLException(String.format("Unmanaged expression: %s", ExpressionEnum.resolveByInstance(expression).name()));
		}
	}
	
	private static void addTermsQuery(BoolQueryBuilder qb, QueryContents queryContents) {
		if(queryContents.getEqualObjects() != null) {
			queryContents.getEqualObjects().forEach((field, values) -> {
				qb.must().add(QueryBuilders.termsQuery(field, values));
			});
		}
		
		if(queryContents.getNotEqualObjects() != null) {
			queryContents.getNotEqualObjects().forEach((field, values) -> {
				qb.mustNot().add(QueryBuilders.termsQuery(field, values));
			});
		}
	}

	private static QueryBuilder evaluateOperatorExpression(Expression expression, SqlStatementSelect select) throws SQLException {
		switch(ExpressionEnum.resolveByInstance(expression)) {
			/*case AND_EXPRESSION:
				AndExpression andExpression = (AndExpression)expression;
				BoolQueryBuilder must = QueryBuilders.boolQuery();
				must.must().add(evaluateQueryExpression(andExpression.getLeftExpression(), select));
				must.must().add(evaluateQueryExpression(andExpression.getRightExpression(), select));
				return must;
			case OR_EXPRESSION:
				OrExpression orExpression = (OrExpression)expression;
				BoolQueryBuilder should = QueryBuilders.boolQuery();
				should.should().add(evaluateQueryExpression(orExpression.getLeftExpression(), select));
				should.should().add(evaluateQueryExpression(orExpression.getRightExpression(), select));
				return should;
			case PARENTHESIS:
				Parenthesis parenthesis = (Parenthesis)expression;
				return evaluateQueryExpression(parenthesis.getExpression(), select);*/
			case GREATER_THAN:
				GreaterThan greaterThan = (GreaterThan)expression;
				return QueryBuilders.rangeQuery(getColumn(greaterThan.getLeftExpression())).gt(evaluateValueExpression(greaterThan.getRightExpression()));
			case GREATER_THAN_EQUALS:
				GreaterThanEquals greaterThanEquals = (GreaterThanEquals)expression;
				return QueryBuilders.rangeQuery(getColumn(greaterThanEquals.getLeftExpression())).gte(evaluateValueExpression(greaterThanEquals.getRightExpression()));
			case MINOR_THAN:
				MinorThan minorThan = (MinorThan)expression;
				return QueryBuilders.rangeQuery(getColumn(minorThan.getLeftExpression())).lt(evaluateValueExpression(minorThan.getRightExpression()));
			case MINOR_THAN_EQUALS:
				MinorThanEquals minorThanEquals = (MinorThanEquals)expression;
				return QueryBuilders.rangeQuery(getColumn(minorThanEquals.getLeftExpression())).lte(evaluateValueExpression(minorThanEquals.getRightExpression()));
			case EQUALS_TO:
				EqualsTo equalsTo = (EqualsTo)expression;
				return QueryBuilders.termQuery(getColumn(equalsTo.getLeftExpression()), evaluateValueExpression(equalsTo.getRightExpression()));
			case NOT_EQUALS_TO:
				NotEqualsTo notEqualsTo = (NotEqualsTo)expression;
				return QueryBuilders.termQuery(getColumn(notEqualsTo.getLeftExpression()), evaluateValueExpression(notEqualsTo.getRightExpression()));
			case IS_NULL_EXPRESSION:
 				IsNullExpression isNullExpression = (IsNullExpression)expression;
 				QueryBuilder isNullQueryBuilder = QueryBuilders.existsQuery(getColumn(isNullExpression.getLeftExpression()));
 				if(isNullExpression.isNot()) {
 					BoolQueryBuilder mustNot = QueryBuilders.boolQuery();
 					mustNot.mustNot().add(isNullQueryBuilder);
 					return mustNot;
 				}
				return isNullQueryBuilder;
			case NOT_EXPRESSION:
				NotExpression notExpression = (NotExpression)expression;
				BoolQueryBuilder mustNot = QueryBuilders.boolQuery();
				mustNot.mustNot().add(evaluateQueryExpression(notExpression.getExpression(), select, null));
				return mustNot;
			default:
				throw new SQLException(String.format("Unmanaged expression: %s", ExpressionEnum.resolveByInstance(expression).name()));
		}
	}
	
	private static Object evaluateValueExpression(Expression expression) throws SQLException {
		switch(ExpressionEnum.resolveByInstance(expression)) {	
			case DOUBLE_VALUE:
				DoubleValue doubleValue = (DoubleValue)expression;
				return doubleValue.getValue();
			case STRING_VALUE:
				StringValue stringValue = (StringValue)expression;
				return stringValue.getValue();			
			case LONG_VALUE:
				LongValue longValue = (LongValue)expression;
				return longValue.getValue();
			case FUNCTION:
				return resolveFunction((Function)expression);
			case ADDITION:
				Addition addition = (Addition)expression;
				return addition(evaluateValueExpression(addition.getLeftExpression()), evaluateValueExpression(addition.getRightExpression()));
			case SUBTRACTION:
				Subtraction subraction = (Subtraction)expression;
				return subtraction(evaluateValueExpression(subraction.getLeftExpression()), evaluateValueExpression(subraction.getRightExpression()));
			case DIVISION:
				Division division = (Division)expression;
				return division(evaluateValueExpression(division.getLeftExpression()), evaluateValueExpression(division.getRightExpression()));
			case MULTIPLICATION:
				Multiplication multiplication = (Multiplication)expression;
				return multiplication(evaluateValueExpression(multiplication.getLeftExpression()), evaluateValueExpression(multiplication.getRightExpression()));
			default:
				throw new SQLException(String.format("Unmanaged expression: %s", ExpressionEnum.resolveByInstance(expression).name()));
		}
	}
	
	private static Object resolveFunction(Function function) throws SQLException {
		ExpressionList parameters = (ExpressionList)function.getParameters();
		
		switch(function.getName().toUpperCase()) {
			case "TO_DATE":
				if(parameters.getExpressions().size() != 2) {
					throw new SQLSyntaxErrorException("TO_DATE with invalid number of parameters");
				}
				return ToDateUtils.resolveToDate((String)evaluateValueExpression(parameters.getExpressions().get(0)), (String)evaluateValueExpression(parameters.getExpressions().get(1)));
		}
		return null;
	}

	private static String getColumn(Expression expression) throws SQLException {
		if(!(expression instanceof Column)) {
			throw new SQLException(String.format("Not Column expression: %s", ExpressionEnum.resolveByInstance(expression).name()));
		}
		
		Column column = (Column)expression;
		return column.getColumnName().replace("\"", "");
	}



	private static Object addition(Object a, Object b) {
		if(a instanceof Long && b instanceof Long) {
			return (Long)a + (Long)b;
		}

		if(a instanceof Long && b instanceof Double) {
			return (Long)a + (Double)b;
		}
		
		if(a instanceof Double && b instanceof Long) {
			return (Double)a + (Long)b;
		}

		if(a instanceof Double && b instanceof Double) {
			return (Double)a + (Double)b;
		}

		return null;
	}

	private static Object subtraction(Object a, Object b) {
		if(a instanceof Long && b instanceof Long) {
			return (Long)a - (Long)b;
		}

		if(a instanceof Long && b instanceof Double) {
			return (Long)a - (Double)b;
		}
		
		if(a instanceof Double && b instanceof Long) {
			return (Double)a - (Long)b;
		}

		if(a instanceof Double && b instanceof Double) {
			return (Double)a - (Double)b;
		}

		return null;
	}

	private static Object division(Object a, Object b) {
		if(a instanceof Long && b instanceof Long) {
			return (Long)a / (Long)b;
		}

		if(a instanceof Long && b instanceof Double) {
			return (Long)a / (Double)b;
		}
		
		if(a instanceof Double && b instanceof Long) {
			return (Double)a / (Long)b;
		}

		if(a instanceof Double && b instanceof Double) {
			return (Double)a / (Double)b;
		}

		return null;
	}
	
	private static Object multiplication(Object a, Object b) {
		if(a instanceof Long && b instanceof Long) {
			return (Long)a * (Long)b;
		}

		if(a instanceof Long && b instanceof Double) {
			return (Long)a * (Double)b;
		}
		
		if(a instanceof Double && b instanceof Long) {
			return (Double)a * (Long)b;
		}

		if(a instanceof Double && b instanceof Double) {
			return (Double)a * (Double)b;
		}

		return null;
	}
}

package org.fpasti.jdbc.esqlj.elastic.query.impl.search.clause;

import java.sql.SQLSyntaxErrorException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.fpasti.jdbc.esqlj.elastic.query.impl.search.clause.utils.ExpressionResolverValue;
import org.fpasti.jdbc.esqlj.elastic.query.statement.SqlStatementSelect;
import org.fpasti.jdbc.esqlj.elastic.query.statement.model.ExpressionEnum;
import org.fpasti.jdbc.esqlj.elastic.query.statement.model.QueryColumn;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;

public class ClauseHaving {

	public static void manageHavingCondition(SqlStatementSelect select, TermsAggregationBuilder deeperTermsAggregationBuilder) throws SQLSyntaxErrorException {
		String bucketSelectorScript = evaluateHavingExpression(select.getHavingCondition(), select);
		
		Map<String, String> params = new HashMap<String, String>();
		
		for(Integer idx = 0; idx < select.getQueryColumns().size(); idx++) {
			if(select.getQueryColumns().get(idx).getAggregatingFunctionExpression() != null) {
				params.put(idx.toString(), idx.toString());
			}
		}
		
		deeperTermsAggregationBuilder.subAggregation(PipelineAggregatorBuilders.bucketSelector("bucket_filter", params, new Script(bucketSelectorScript)));
	}
	
	private static String evaluateHavingExpression(Expression expression, SqlStatementSelect select) throws SQLSyntaxErrorException {
		switch(ExpressionEnum.resolveByInstance(expression)) {
			case AND_EXPRESSION:
				AndExpression andExpression = (AndExpression)expression;
				String leftAnd = evaluateHavingExpression(andExpression.getLeftExpression(), select);
				String rightAnd = evaluateHavingExpression(andExpression.getRightExpression(), select);
				return String.format("%s && %s", leftAnd, rightAnd);
			case OR_EXPRESSION:
				OrExpression orExpression = (OrExpression)expression;
				String leftOr = evaluateHavingExpression(orExpression.getLeftExpression(), select);
				String rightOr = evaluateHavingExpression(orExpression.getRightExpression(), select);				
				return String.format("%s || %s", leftOr, rightOr);
			case PARENTHESIS:
				Parenthesis parenthesis = (Parenthesis)expression;
				return String.format("(%s)", evaluateHavingExpression(parenthesis.getExpression(), select));
			case GREATER_THAN:
				GreaterThan greaterThan = (GreaterThan)expression;
				return String.format("%s>%s", getColumn(greaterThan.getLeftExpression(), select), formatValue(ExpressionResolverValue.evaluateValueExpression(greaterThan.getRightExpression())));
			case GREATER_THAN_EQUALS:
				GreaterThanEquals greaterThanEquals = (GreaterThanEquals)expression;
				return String.format("%s>=%s", getColumn(greaterThanEquals.getLeftExpression(), select), formatValue(ExpressionResolverValue.evaluateValueExpression(greaterThanEquals.getRightExpression())));
			case MINOR_THAN:
				MinorThan minorThan = (MinorThan)expression;
				return String.format("%s<%s", getColumn(minorThan.getLeftExpression(), select), formatValue(ExpressionResolverValue.evaluateValueExpression(minorThan.getRightExpression())));
			case MINOR_THAN_EQUALS:
				MinorThanEquals minorThanEquals = (MinorThanEquals)expression;
				return String.format("%s<=%s", getColumn(minorThanEquals.getLeftExpression(), select), formatValue(ExpressionResolverValue.evaluateValueExpression(minorThanEquals.getRightExpression())));
			case EQUALS_TO:
				EqualsTo equalsTo = (EqualsTo)expression;
				return String.format("%s==%s", getColumn(equalsTo.getLeftExpression(), select), formatValue(ExpressionResolverValue.evaluateValueExpression(equalsTo.getRightExpression())));
			case NOT_EQUALS_TO:
				NotEqualsTo notEqualsTo = (NotEqualsTo)expression;
				return String.format("%s!=%s", getColumn(notEqualsTo.getLeftExpression(), select), formatValue(ExpressionResolverValue.evaluateValueExpression(notEqualsTo.getRightExpression())));
			case IS_NULL_EXPRESSION:
 				IsNullExpression isNullExpression = (IsNullExpression)expression;
				return String.format("%s==null", getColumn(isNullExpression.getLeftExpression(), select));
			case NOT_EXPRESSION:
				NotExpression notExpression = (NotExpression)expression;
				return String.format("!(%s)", evaluateHavingExpression(notExpression.getExpression(), select));
			case BETWEEN:
				Between between = (Between)expression;
				return String.format("%s>=%s AND %s<=%s", getColumn(between.getLeftExpression(), select), formatValue(ExpressionResolverValue.evaluateValueExpression(between.getBetweenExpressionStart())), getColumn(between.getLeftExpression(), select), ExpressionResolverValue.evaluateValueExpression(between.getBetweenExpressionEnd()));
			default:
				throw new SQLSyntaxErrorException(String.format("Unmanaged expression: %s", ExpressionEnum.resolveByInstance(expression).name()));
		}
	}
	
	private static String formatValue(Object value) {
		if(value instanceof String) {
			return String.format("'%s'", value);
		}
		return value.toString();
	}

	private static String getColumn(Expression expression, SqlStatementSelect select) throws SQLSyntaxErrorException {
		String columnName = null;
		
		if(expression instanceof Column) {
			Column column = (Column)expression;
			columnName = column.getColumnName().replace("\"", "");
		} else if(expression instanceof Function) {
			columnName = expression.toString();			
		} else {
			throw new SQLSyntaxErrorException(String.format("Undeclared column '%s' in Select clause", ExpressionEnum.resolveByInstance(expression).name()));
		}
		
		QueryColumn queryColumn = select.getColumnsByNameOrAliasOrAggregatingFunction(columnName);		
		int columnIdx = resolveColumnIndex(queryColumn, select);
		return String.format("params.%d", columnIdx);
	}
	
	private static int resolveColumnIndex(QueryColumn columnField, SqlStatementSelect select) throws SQLSyntaxErrorException {
		return IntStream.range(0, select.getQueryColumns().size()).filter(idx -> select.getQueryColumns().get(idx).getName().equals(columnField.getName())).findFirst().getAsInt();
	}
}

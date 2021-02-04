package org.takeshi.jdbc.esqlj.elastic.query.impl.search;

import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.Date;

import org.joda.time.LocalDate;
import org.takeshi.jdbc.esqlj.elastic.query.statement.model.ExpressionEnum;
import org.takeshi.jdbc.esqlj.support.ToDateUtils;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class ValueExpressionResolver {

	public static Object evaluateValueExpression(Expression expression) throws SQLException {
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
			case "NOW":
			case "GETDATE":
				return new Date();
			case "CURDATE":
				return new LocalDate(new Date()).toDate();
		}
		throw new SQLSyntaxErrorException(String.format("Function '%s' unsupported", function.getName()));
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

package org.fpasti.jdbc.esqlj.elastic.query.statement;

import java.sql.SQLSyntaxErrorException;

import org.fpasti.jdbc.esqlj.elastic.query.statement.model.ExpressionEnum;
import org.fpasti.jdbc.esqlj.elastic.query.statement.model.FunctionEnum;
import org.fpasti.jdbc.esqlj.support.EsRuntimeException;
import org.fpasti.jdbc.esqlj.support.EsWrapException;

import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class StatementUtils {

	public static boolean isExpressionEquals(Object epxressionInstance, ExpressionEnum expressionEnum) {
		return ExpressionEnum.resolveByInstance(epxressionInstance).equals(expressionEnum);
	}
	
	public static FunctionEnum resolveFunctionType(Function function) {
		try {
			return FunctionEnum.valueOf(function.getName().toUpperCase());
		} catch(IllegalArgumentException e) {
			throw new EsRuntimeException(String.format("Unsupported select function '%s'", function.getName()));
		}
	}
	
	public static String resolveFunctionColumnName(FunctionEnum functionType, Function functionExpression) {
		switch(functionType) {
			case LONGITUDE:
			case LATITUDE:
			case TO_CHAR:
				return ((Column)functionExpression.getParameters().getExpressions().get(0)).getColumnName().replace("\"", "");
			case COUNT:
				return functionExpression.getParameters() == null ? functionExpression.toString() : functionExpression.getParameters().getExpressions().get(0).toString().replace("\"", "");
			case AVG:
			case SUM:
			case MIN:
			case MAX:
				if(functionExpression.isAllColumns()) {
					throw new EsWrapException(new SQLSyntaxErrorException(String.format("Unsupported '*' operator on expression: %s", functionExpression.toString())));
				}
				return functionExpression.getParameters().getExpressions().get(0).toString().replace("\"", "");
			default:
				throw new EsRuntimeException(String.format("Unsupported select function '%s'", functionExpression.getName()));
		}
	}
	
}

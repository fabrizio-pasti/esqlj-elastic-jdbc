package org.fpasti.jdbc.esqlj.elastic.query.statement;

import org.fpasti.jdbc.esqlj.elastic.query.statement.model.ExpressionEnum;
import org.fpasti.jdbc.esqlj.support.EsRuntimeException;

import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class StatementUtils {

	public static boolean isExpressionEquals(Object epxressionInstance, ExpressionEnum expressionEnum) {
		return ExpressionEnum.resolveByInstance(epxressionInstance).equals(expressionEnum);
	}
	
	public static String resolveFunctionColumnName(Function function) {
		switch(function.getName().toUpperCase()) {
			case "TO_CHAR":
				return ((Column)function.getParameters().getExpressions().get(0)).getColumnName();
			case "COUNT":
				return function.toString();
			default:
				throw new EsRuntimeException(String.format("Unsupported select function '%s'", function.getName()));
		}
	}
	
}

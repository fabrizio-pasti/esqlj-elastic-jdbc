package org.fpasti.jdbc.esqlj.elastic.query.statement;

import java.sql.SQLSyntaxErrorException;

import org.fpasti.jdbc.esqlj.elastic.model.ElasticFieldType;
import org.fpasti.jdbc.esqlj.elastic.query.statement.model.ExpressionEnum;
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
	
	public static String resolveFunctionColumnName(Function function) {
		switch(function.getName().toUpperCase()) {
			case "TO_CHAR":
				return ((Column)function.getParameters().getExpressions().get(0)).getColumnName();
			case "COUNT":
				return function.getParameters() == null ? function.toString() : function.getParameters().getExpressions().get(0).toString().replace("\"", "");
			case "AVG":
			case "SUM":
				if(function.isAllColumns()) {
					throw new EsWrapException(new SQLSyntaxErrorException(String.format("Unsupported '*' operator on expression: %s", function.toString())));
				}
				return function.getParameters().getExpressions().get(0).toString().replace("\"", "");
			default:
				throw new EsRuntimeException(String.format("Unsupported select function '%s'", function.getName()));
		}
	}
	
	public static ElasticFieldType resolveAggregationType(Function function) {
		switch(function.getMultipartName().get(0)) {
			case "COUNT":
				return ElasticFieldType.LONG;
			case "AVG":
			case "SUM":
				return ElasticFieldType.DOUBLE;
		}
		
		throw new EsWrapException(new SQLSyntaxErrorException(String.format("Unsupported aggregating function '%s'", function.getName())));
	}
	
}

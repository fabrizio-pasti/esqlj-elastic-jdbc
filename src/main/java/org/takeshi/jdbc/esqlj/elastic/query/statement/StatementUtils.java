package org.takeshi.jdbc.esqlj.elastic.query.statement;

import org.takeshi.jdbc.esqlj.elastic.query.statement.model.ExpressionEnum;

public class StatementUtils {

	public static boolean isExpressionEquals(Object epxressionInstance, ExpressionEnum expressionEnum) {
		return ExpressionEnum.resolveByInstance(epxressionInstance).equals(expressionEnum);
	}
}

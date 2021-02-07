package org.fpasti.jdbc.esqlj.elastic.query.statement.formatter;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.StringValue;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public abstract class Formatter {
	private Function function;
	
	public Formatter(Function function) {
		this.function = function;
		init();
	}

	public abstract Object resolveValue(Object value);
	
	protected abstract void init();
	
	public Function getFunction() {
		return function;
	}
	
	protected String getStrValueParameter(int idx) {
		return ((StringValue)getParameter(idx)).getValue();
	}
	
	private Expression getParameter(int idx) {
		return function.getParameters().getExpressions().get(idx);
	}
}

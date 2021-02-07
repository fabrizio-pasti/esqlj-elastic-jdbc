package org.fpasti.jdbc.esqlj.elastic.query.statement.model;

import org.fpasti.jdbc.esqlj.elastic.query.statement.StatementUtils;
import org.fpasti.jdbc.esqlj.elastic.query.statement.formatter.Formatter;
import org.fpasti.jdbc.esqlj.elastic.query.statement.formatter.FormatterFactory;

import net.sf.jsqlparser.expression.Function;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class Field {
	private String name;
	private String alias;
	private String index;
	private Function function;
	private Formatter formatter;
	
	public Field(String name, String alias, String index) {
		this.name = name.replace("\"", "");
		this.alias = alias != null ? alias.replace("\"", "") : null;
		this.index = index;
	}

	public Field(Function function, String alias) {
		this.name = StatementUtils.resolveFunctionColumn(function);
		this.function = function;
		this.formatter = FormatterFactory.getFormatter(function);
		this.alias = alias != null ? alias : function.toString().replaceAll(" ", "");
	}

	public String getName() {
		return name;
	}

	public String getAlias() {
		return alias;
	}

	public String getIndex() {
		return index;
	}

	public Function getFunction() {
		return function;
	}

	public Formatter getFormatter() {
		return formatter;
	}

}

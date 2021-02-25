package org.fpasti.jdbc.esqlj.elastic.query.statement.model;

import org.fpasti.jdbc.esqlj.elastic.model.ElasticFieldType;
import org.fpasti.jdbc.esqlj.elastic.query.statement.StatementUtils;
import org.fpasti.jdbc.esqlj.elastic.query.statement.formatter.Formatter;
import org.fpasti.jdbc.esqlj.elastic.query.statement.formatter.FormatterFactory;

import net.sf.jsqlparser.expression.Function;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class QueryColumn {
	private String name;
	private String alias;
	private String index;
	private Function aggregatingFunctionExpression;
	private Formatter formatter;
	private FunctionEnum functionType;
	
	public QueryColumn(String name, String alias, String index) {
		this.name = name.replace("\"", "");
		this.alias = alias != null ? alias.replace("\"", "") : null;
		this.index = index;
	}

	public QueryColumn(Function functionExpression, String alias) {
		this.functionType = StatementUtils.resolveFunctionType(functionExpression);
		this.name = StatementUtils.resolveFunctionColumnName(functionType, functionExpression);

		this.formatter = FormatterFactory.getFormatter(functionType, functionExpression);
		if(functionExpression != null && formatter == null) {
			this.aggregatingFunctionExpression = functionExpression;
		}
		this.alias = alias != null ? alias : functionExpression.toString().replaceAll(" ", "");
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

	public Function getAggregatingFunctionExpression() {
		return aggregatingFunctionExpression;
	}

	public Formatter getFormatter() {
		return formatter;
	}

	public ElasticFieldType getAggregatingType() {
		return functionType != null ? functionType.getType() : null;
	}

	public String getAggregatingColumnName() {
		return aggregatingFunctionExpression != null ? aggregatingFunctionExpression.toString() : null;
	}
	
	public FunctionEnum getFunctionType() {
		return functionType;
	}

}

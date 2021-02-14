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
	private Function aggregatingFunction;
	private Formatter formatter;
	private ElasticFieldType aggregatingType;
	private String aggregatingColumnName;
	
	public QueryColumn(String name, String alias, String index) {
		this.name = name.replace("\"", "");
		this.alias = alias != null ? alias.replace("\"", "") : null;
		this.index = index;
	}

	public QueryColumn(Function function, String alias) {
		this.name = StatementUtils.resolveFunctionColumnName(function);
		
		this.formatter = FormatterFactory.getFormatter(function);
		if(function != null && formatter == null) {
			this.aggregatingFunction = function;
			this.aggregatingType = StatementUtils.resolveAggregationType(function);
			this.aggregatingColumnName = function.toString(); // transform invalid characters if required
		}
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

	public Function getAggregatingFunction() {
		return aggregatingFunction;
	}

	public Formatter getFormatter() {
		return formatter;
	}

	public ElasticFieldType getAggregatingType() {
		return aggregatingType;
	}

	public String getAggregatingColumnName() {
		return aggregatingColumnName;
	}

}

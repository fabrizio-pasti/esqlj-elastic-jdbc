package org.fpasti.jdbc.esqlj.elastic.query.statement;

import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.fpasti.jdbc.esqlj.elastic.query.statement.model.Index;
import org.fpasti.jdbc.esqlj.elastic.query.statement.model.QueryColumn;
import org.fpasti.jdbc.esqlj.elastic.query.statement.model.QueryType;
import org.fpasti.jdbc.esqlj.support.EsRuntimeException;
import org.fpasti.jdbc.esqlj.support.EsWrapException;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class SqlStatementSelect extends SqlStatement {

	private PlainSelect select;
	private List<Index> indices;
	private List<QueryColumn> queryColumns;
	private List<OrderByElement> orderByElements;
	private List<String> groupByColumns;
	private QueryType queryType = QueryType.DOCS;
	
	public SqlStatementSelect(Statement statement) {
		super(SqlStatementType.SELECT);
		select = (PlainSelect)((Select)statement).getSelectBody();
		
		init();
	}
	
	public Long getLimit() {
		return select.getLimit() != null ? ((LongValue)select.getLimit().getRowCount()).getBigIntegerValue().longValue() : null;
	}

	public List<QueryColumn> getQueryColumns() {
		return queryColumns;
	}
	
	public List<OrderByElement> getOrderByElements() {
		return orderByElements;
	}

	public Expression getWhereCondition() {
		return select.getWhere();
	}
	
	public Index getIndexByNameOrAlias(String name) {
		return indices.stream().filter(index -> index.getName().equals(name) || (index.getAlias() != null && index.getAlias().equals(name))).findFirst().orElse(null);
	}

	public QueryColumn getColumnsByNameOrAlias(String name) {
		return queryColumns.stream().filter(field -> field.getName().equals(name) || (field.getAlias() != null && field.getAlias().equals(name))).findFirst().orElse(null);
	}
	
	public List<String> getGroupByColumns() {
		return groupByColumns;
	}

	public QueryType getQueryType() {
		return queryType;
	}

	private void init() {
		index = new Index(select.getFromItem().toString().replaceAll("\"", ""), select.getFromItem().getAlias() != null ? select.getFromItem().getAlias().getName().replaceAll("\"", "") : null);
		indices = new ArrayList<Index>(); 
		indices.add(index);
		if(select.getJoins() != null) {
			indices.addAll(select.getJoins().stream().map(join -> new Index(((Table)((Join)join).getRightItem()).getName(), ((Table)((Join)join).getRightItem()).getAlias() != null ? ((Table)((Join)join).getRightItem()).getAlias().getName()  : null)).collect(Collectors.toList()));
		}
		
		queryColumns = resolveColumns();
		orderByElements = resolveOrderByElements();
		groupByColumns = select.getGroupBy() != null ? select.getGroupBy().getGroupByExpressions().stream().map(expression -> ((Column)expression).getColumnName()).collect(Collectors.toList()) : new ArrayList<String>();
		resolveAggregationType();
	}

	private List<QueryColumn> resolveColumns() {
		String index = "root";
		return select.getSelectItems().stream().map(item -> {
			if(item instanceof SelectExpressionItem) {
				SelectExpressionItem sei = (SelectExpressionItem)item;
				if(sei.getExpression() instanceof Column) {
					Column c = (Column)sei.getExpression();
					return new QueryColumn(c.getColumnName(), sei.getAlias() == null ? null : sei.getAlias().getName(), c.getTable() == null ? index : getIndexByNameOrAlias(c.getTable().getName().replace("\"", "")).getName());
				} else if(sei.getExpression() instanceof Function) {
					Function f = (Function)sei.getExpression();
					return new QueryColumn(f, sei.getAlias() == null ? null : sei.getAlias().getName());				
				}
			} else if(item instanceof AllColumns) {
				return new QueryColumn("*", null, index);
			}
			throw new EsRuntimeException(String.format("Unexpected expression '%s' in select clause", item.toString()));
		}).collect(Collectors.toList());
	}
	
	private List<OrderByElement> resolveOrderByElements() {
		if(select.getOrderByElements() == null) {
			return new ArrayList<OrderByElement>();
		}
		
		return select.getOrderByElements().stream().map(elem -> {
			Column c = (Column)elem.getExpression();
			QueryColumn columnOrAlias = getColumnsByNameOrAlias(c.getColumnName());
			if(columnOrAlias != null) {
				c.setColumnName(columnOrAlias.getName());
			} 
			return elem;
		}).collect(Collectors.toList());
	}
		
	private void resolveAggregationType() {
		if(getGroupByColumns().size() > 0) {
			queryType = QueryType.AGGR_GROUPED;
			return;
		}		
		
		if(getQueryColumns().get(0).getAggregatingFunction() == null) {
			return;
		}
		
		if(!getQueryColumns().get(0).getAggregatingFunction().getName().equalsIgnoreCase("COUNT")) {
			throw new EsWrapException(new SQLSyntaxErrorException(String.format("'%s' expressions require 'GROUP BY' expression", getQueryColumns().get(0).getAggregatingFunction().getName())));
		}
		
		queryType = getQueryColumns().get(0).getAggregatingFunction().isAllColumns() ? QueryType.AGGR_COUNT_ALL : QueryType.AGGR_COUNT_FIELD;
		
		if(queryType.equals(QueryType.AGGR_COUNT_ALL)) {
			throw new EsWrapException(new SQLSyntaxErrorException(String.format("COUNT(*) cannot be mixed with other selectors", getQueryColumns().get(0).getAggregatingFunction().getName())));
		}
	}
}

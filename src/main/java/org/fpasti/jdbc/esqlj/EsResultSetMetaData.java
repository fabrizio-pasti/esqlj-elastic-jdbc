package org.fpasti.jdbc.esqlj;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import org.fpasti.jdbc.esqlj.elastic.model.ElasticFieldType;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class EsResultSetMetaData implements ResultSetMetaData {

	private List<ElasticFieldType> columnTypes;
	private String source;
	private List<String> labelNames;
	private List<String> columnNames;
	
	
	public EsResultSetMetaData(String source, List<String> labelNames, List<ElasticFieldType> columnTypes) {
		this.source = source;
		this.labelNames = labelNames;
		this.columnTypes = columnTypes;
	}
	
	public EsResultSetMetaData(String source, List<String> labelNames, List<String> columnNames, List<ElasticFieldType> columnTypes) {
		this(source, labelNames, columnTypes);
		this.columnNames = columnNames;
	}

	@Override
	public int getColumnType(int column) throws SQLException { 
		return columnTypes.get(column - 1).getSqlTypeCode();
	}
	
	@Override
	public String getColumnTypeName(int column) throws SQLException {
		return columnTypes.get(column - 1).getSqlType();
	}
	
	@Override
	public String getColumnClassName(int column) throws SQLException {
		return columnTypes.get(column - 1).getClazz().getName();
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return iface.cast(this);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return iface.isInstance(this);
	} 

	@Override
	public int getColumnCount() throws SQLException {
		return labelNames.size();
	}

	@Override
	public boolean isAutoIncrement(int column) throws SQLException {
		return true;
	}

	@Override
	public boolean isCaseSensitive(int column) throws SQLException {
		return true;
	}

	@Override
	public boolean isSearchable(int column) throws SQLException {
		return true;
	}

	@Override
	public boolean isCurrency(int column) throws SQLException {
		return false;
	}

	@Override
	public int isNullable(int column) throws SQLException {
		return columnTypes.get(column - 1).isPrimaryKey() ? columnNoNulls : columnNullable;
	}

	@Override
	public boolean isSigned(int column) throws SQLException {
		return false;
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException {
		return 65536;
	}

	@Override
	public String getColumnLabel(int column) throws SQLException {
		return labelNames.get(column - 1);
	}

	@Override
	public String getColumnName(int column) throws SQLException {
		return columnNames.get(column - 1);
	}

	@Override
	public String getSchemaName(int column) throws SQLException {
		return "";
	}

	@Override
	public int getPrecision(int column) throws SQLException {
		return 0;
	}

	@Override
	public int getScale(int column) throws SQLException {
		return 0;
	}

	@Override
	public String getTableName(int column) throws SQLException {
		return source;
	}

	@Override
	public String getCatalogName(int column) throws SQLException {
		return "";
	}

	@Override
	public boolean isReadOnly(int column) throws SQLException {
		return true;
	}

	@Override
	public boolean isWritable(int column) throws SQLException {
		return false;
	}

	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException {
		return false;
	}
}

package org.takeshi.jdbc.esqlj.elastic.query.data;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

public abstract class AbstractResultSetMetaData implements ResultSetMetaData {

	private String source;
	private List<String> columnsName;
	
	public AbstractResultSetMetaData(String source, List<String> columnsName) {
		this.source = source;
		this.columnsName = columnsName;
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
		return columnsName.size();
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
		return columnNullableUnknown;
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
		return columnsName.get(column - 1);
	}

	@Override
	public String getColumnName(int column) throws SQLException {
		return columnsName.get(column - 1);
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
		return false;
	}

	@Override
	public boolean isWritable(int column) throws SQLException {
		return true;
	}

	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException {
		return true;
	}

}

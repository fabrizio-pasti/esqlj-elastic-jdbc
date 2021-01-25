package org.takeshi.jdbc.esqlj;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.takeshi.jdbc.esqlj.elastic.model.ElasticFieldType;
import org.takeshi.jdbc.esqlj.elastic.model.IndexMetaData;
import org.takeshi.jdbc.esqlj.elastic.query.model.DataRow;

public class EsResultSetMetaData implements ResultSetMetaData {

	private List<ElasticFieldType> columnTypes;
	private String source;
	private List<String> columnsName;
	
	public EsResultSetMetaData(String source, List<String> columnsName, List<DataRow> dataRows) {
		this.source = source;
		this.columnsName = columnsName;
		fetchTypesByData(dataRows);
	}
	
	public EsResultSetMetaData(String source, IndexMetaData indexMetaData) {
		this.source = source;
		this.columnsName = indexMetaData.getFieldsName();
		fetchTypesByMetaData(indexMetaData);
	}

	private void fetchTypesByData(List<DataRow> dataRows) {
		if(dataRows == null || dataRows.size() == 0) {
			columnTypes = IntStream.range(0, columnsName.size()).mapToObj(i -> ElasticFieldType.UNKNOWN).collect(Collectors.toList());
		} else {
			columnTypes = IntStream.range(0, columnsName.size()).mapToObj(i -> {
				ElasticFieldType t = ElasticFieldType.UNKNOWN;
				for(int row = 0; row < dataRows.size(); row++) {
					if(dataRows.get(row).data.get(i) != null) {
						t = ElasticFieldType.resolveByValue(dataRows.get(row).data.get(i));
						break;
					}
				}
				return t;
			}).collect(Collectors.toList());
		}
	}

	private void fetchTypesByMetaData(IndexMetaData indexMetaData) {
		columnTypes = indexMetaData.getFields().stream().map(field -> field.getType()).collect(Collectors.toList());
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
		return columnNullable;
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

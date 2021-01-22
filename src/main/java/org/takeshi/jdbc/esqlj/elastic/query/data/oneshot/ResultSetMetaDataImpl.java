package org.takeshi.jdbc.esqlj.elastic.query.data.oneshot;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.takeshi.jdbc.esqlj.elastic.query.data.AbstractResultSetMetaData;
import org.takeshi.jdbc.esqlj.elastic.query.model.ElasticFieldType;

public class ResultSetMetaDataImpl extends AbstractResultSetMetaData {

	private List<ElasticFieldType> columnTypes;
	
	public ResultSetMetaDataImpl(String source, List<String> columnsName, List<DataRow> dataRows) {
		super(source, columnsName);
		fetchTypes(dataRows);
	}

	private void fetchTypes(List<DataRow> dataRows) {
		if(dataRows == null || dataRows.size() == 0) {
			columnTypes = IntStream.range(0, dataRows.size()).mapToObj(i -> ElasticFieldType.UNKNOWN).collect(Collectors.toList());
		} else {
			columnTypes = IntStream.range(0, dataRows.size()).mapToObj(i -> {
				return dataRows.get(0).data.get(i) == null ? ElasticFieldType.UNKNOWN : ElasticFieldType.resolveByValue(dataRows.get(0).data.get(i));
			}).collect(Collectors.toList());
		}
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

}

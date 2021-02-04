package org.takeshi.jdbc.esqlj.elastic.query.impl;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.takeshi.jdbc.esqlj.EsConnection;
import org.takeshi.jdbc.esqlj.EsMetaData;
import org.takeshi.jdbc.esqlj.elastic.model.ElasticField;
import org.takeshi.jdbc.esqlj.elastic.model.ElasticFieldType;
import org.takeshi.jdbc.esqlj.elastic.query.AbstractOneShotQuery;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class IndexFieldsQuery extends AbstractOneShotQuery {
	
	private static String[] COLUMNS =  {"TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"};
			

	public IndexFieldsQuery(EsConnection connection, String tableNamePattern, String columnNamePattern) throws SQLException {
		super(connection, "index_fields", COLUMNS);
		init(tableNamePattern);
	}

	public void init(String index) throws SQLException {
		((EsMetaData)getConnection().getMetaData()).getMetaDataService().getIndexFields(index).forEach((name, field) -> {
			insertRow(index, field);
		});
		insertRow(index, new ElasticField(ElasticField.DOC_ID_ALIAS, ElasticFieldType.DOC_ID));
	}

	private void insertRow(String index, ElasticField field) {
		Map<String, Object> data = new HashMap<String, Object>();
		data.put("TABLE_NAME", index);
		data.put("COLUMN_NAME", field.getFullName());
		data.put("DATA_TYPE", field.getType().getSqlTypeCode());
		data.put("TYPE_NAME", field.getType().getSqlType());
		data.put("NUM_PREC_RADIX", 10);
		data.put("NULLABLE", field.getType().isPrimaryKey() ? ResultSetMetaData.columnNoNulls : ResultSetMetaData.columnNullable);
		data.put("IS_NULLABLE", field.getType().isPrimaryKey() ? "NO" : "YES");
		data.put("IS_AUTOINCREMENT", "NO");
		data.put("SOURCE_DATA_TYPE", field.getType().getSqlTypeCode());
		if(field.getType().equals(ElasticFieldType.KEYWORD) && field.getSize() == null) {
			data.put("COLUMN_SIZE", 32766L);
		} else {
			data.put("COLUMN_SIZE", field.getSize());
		}
		insertRowWithData(data);
	}
	
	
	/*
	 @Override
public GeometryObject getEnvelope(Object geomObj) throws SQLException {
	GeometryObject envelope = null;

	if (geomObj instanceof Struct) {
		JGeometry geometry = JGeometry.loadJS((Struct)geomObj);
		double[] ordinates = geometry.getMBR();
		double[] coordinates;

		if (geometry.getDimensions() == 3)
			coordinates = new double[]{ordinates[0], ordinates[1], ordinates[2], ordinates[3], ordinates[4], ordinates[5]};
		else 
			coordinates = new double[]{ordinates[0], ordinates[1], 0, ordinates[2], ordinates[3], 0};

		envelope = GeometryObject.createEnvelope(coordinates, 3, geometry.getSRID());
	}

	return envelope;
}
 
	 */
}

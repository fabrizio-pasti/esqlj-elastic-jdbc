package org.takeshi.jdbc.esqlj.elastic.query.impl;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.takeshi.jdbc.esqlj.EsConnection;
import org.takeshi.jdbc.esqlj.elastic.query.AbstractOneShotQuery;
import org.takeshi.jdbc.esqlj.elastic.query.model.ElasticField;
import org.takeshi.jdbc.esqlj.elastic.query.model.ElasticFieldType;

public class IndexFieldsQuery extends AbstractOneShotQuery {
	/**
	1.TABLE_CAT String => table catalog (may be null) 
	2.TABLE_SCHEM String => table schema (may be null) 
	3.TABLE_NAME String => table name 
	4.COLUMN_NAME String => column name 
	5.DATA_TYPE int => SQL type from java.sql.Types 
	6.TYPE_NAME String => Data source dependent type name,for a UDT the type name is fully qualified 
	7.COLUMN_SIZE int => column size. 
	8.BUFFER_LENGTH is not used. 
	9.DECIMAL_DIGITS int => the number of fractional digits. Null is returned for data types whereDECIMAL_DIGITS is not applicable. 
	10.NUM_PREC_RADIX int => Radix (typically either 10 or 2) 
	11.NULLABLE int => is NULL allowed. ◦ columnNoNulls - might not allow NULL values 
	◦ columnNullable - definitely allows NULL values 
	◦ columnNullableUnknown - nullability unknown 

	12.REMARKS String => comment describing column (may be null) 
	13.COLUMN_DEF String => default value for the column, which should be interpreted as a string when the value is enclosed in single quotes (may be null) 
	14.SQL_DATA_TYPE int => unused 
	15.SQL_DATETIME_SUB int => unused 
	16.CHAR_OCTET_LENGTH int => for char types themaximum number of bytes in the column 
	17.ORDINAL_POSITION int => index of column in table(starting at 1) 
	18.IS_NULLABLE String => ISO rules are used to determine the nullability for a column. ◦ YES --- if the column can include NULLs 
	◦ NO --- if the column cannot include NULLs 
	◦ empty string --- if the nullability for thecolumn is unknown 

	19.SCOPE_CATALOG String => catalog of table that is the scopeof a reference attribute (null if DATA_TYPE isn't REF) 
	20.SCOPE_SCHEMA String => schema of table that is the scopeof a reference attribute (null if the DATA_TYPE isn't REF) 
	21.SCOPE_TABLE String => table name that this the scopeof a reference attribute (null if the DATA_TYPE isn't REF) 
	22.SOURCE_DATA_TYPE short => source type of a distinct type or user-generatedRef type, SQL type from java.sql.Types (null if DATA_TYPEisn't DISTINCT or user-generated REF) 
	23.IS_AUTOINCREMENT String => Indicates whether this column is auto incremented ◦ YES --- if the column is auto incremented 
	◦ NO --- if the column is not auto incremented 
	◦ empty string --- if it cannot be determined whether the column is auto incremented 

	24.IS_GENERATEDCOLUMN String => Indicates whether this is a generated column ◦ YES --- if this a generated column 
	◦ NO --- if this not a generated column 
	◦ empty string --- if it cannot be determined whether this is a generated column 
	*/
	
	private static String[] COLUMNS =  {"TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"};
			

	public IndexFieldsQuery(EsConnection connection, String tableNamePattern, String columnNamePattern) throws SQLException {
		super(connection, "index_fields", COLUMNS);
		init(tableNamePattern);
	}

	public void init(String index) throws SQLException {
		List<ElasticField> fields = retrieveIndexFields(index);
		
		insertRow(index, new ElasticField("_id", ElasticFieldType.KEYWORD));
		
		fields.forEach(field -> {
			insertRow(index, field);
		});
	}

	private void insertRow(String index, ElasticField field) {
		Map<String, Object> data = new HashMap<String, Object>();
		data.put("TABLE_NAME", index);
		data.put("COLUMN_NAME", field.getName());
		data.put("DATA_TYPE", field.getType().getSqlTypeCode());
		data.put("TYPE_NAME", field.getType().getSqlType());
		insertRowWithData(data);
	}
	
	
	@SuppressWarnings("unchecked")
	private List<ElasticField> retrieveIndexFields(String index) throws SQLException {
		try {
			GetIndexRequest request = new GetIndexRequest(index);
			GetIndexResponse response = getConnection().getElasticClient().indices().get(request, RequestOptions.DEFAULT);
			MappingMetadata indexMappings = response.getMappings().get(index);
			if(indexMappings == null) {
				// TODO: get fields from alias declared in templates. Searching aliases on every index would be a bad idea...
				return new ArrayList<ElasticField>();
			}
			Map<String, Object> indexTypeMappings = indexMappings.getSourceAsMap();
			return analyzeIndexProperties((Map<String, Object>) indexTypeMappings.get("properties"), new ArrayList<ElasticField>(), null);
		} catch(IOException exception) {
			throw new SQLException(String.format("Failed to retrieve fields index %s", index));
		}
	}

	@SuppressWarnings("unchecked")
	private List<ElasticField> analyzeIndexProperties(Map<String, Object> properties, List<ElasticField> fields, String objectName) {
		properties.forEach((key, obj) -> {
			Map<String, Object> element = (Map<String, Object>) obj;
			if(element.get("properties") != null) {
				analyzeIndexProperties((Map<String, Object>)element.get("properties"), fields, objectName == null || objectName.length() == 0 ? key : String.format("%s.%s", objectName, key));
			}
			if(element.get("type") != null) {
				String type = (String)element.get("type");
				try {
					fields.add(new ElasticField(objectName == null || objectName.length() == 0 ? key : String.format("%s.%s", objectName, key), ElasticFieldType.resolveByElasticType(type)));
				} catch(Exception e) {
					System.out.println(String.format("Type %s not supported", type));
				}
			}
		});
		
		return fields.stream().sorted().collect(Collectors.toList());
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

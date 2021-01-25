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
import org.takeshi.jdbc.esqlj.EsMetaData;
import org.takeshi.jdbc.esqlj.elastic.model.ElasticField;
import org.takeshi.jdbc.esqlj.elastic.model.ElasticFieldType;
import org.takeshi.jdbc.esqlj.elastic.query.AbstractOneShotQuery;

public class IndexFieldsQuery extends AbstractOneShotQuery {
	
	private static String[] COLUMNS =  {"TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"};
			

	public IndexFieldsQuery(EsConnection connection, String tableNamePattern, String columnNamePattern) throws SQLException {
		super(connection, "index_fields", COLUMNS);
		init(tableNamePattern);
	}

	public void init(String index) throws SQLException {
		List<ElasticField> fields = ((EsMetaData)getConnection().getMetaData()).getMetaDataService().getIndexFields(index);
		
		insertRow(index, new ElasticField("_id", ElasticFieldType.KEYWORD));
		
		fields.forEach(field -> {
			insertRow(index, field);
		});
	}

	private void insertRow(String index, ElasticField field) {
		Map<String, Object> data = new HashMap<String, Object>();
		data.put("TABLE_NAME", index);
		data.put("COLUMN_NAME", field.getFullName());
		data.put("DATA_TYPE", field.getType().getSqlTypeCode());
		data.put("TYPE_NAME", field.getType().getSqlType());
		data.put("NUM_PREC_RADIX", 10);
		data.put("NULLABLE", 1);
		data.put("IS_NULLABLE", "YES");
		data.put("IS_AUTOINCREMENT", "NO");
		data.put("SOURCE_DATA_TYPE", field.getType().getSqlTypeCode());
		if(field.getType().equals(ElasticFieldType.KEYWORD) && field.getSize() == null) {
			data.put("COLUMN_SIZE", 32766L);
		} else {
			data.put("COLUMN_SIZE", field.getSize());
		}
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

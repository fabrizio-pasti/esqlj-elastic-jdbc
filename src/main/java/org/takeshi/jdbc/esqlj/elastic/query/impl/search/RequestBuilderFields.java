package org.takeshi.jdbc.esqlj.elastic.query.impl.search;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.takeshi.jdbc.esqlj.Configuration;
import org.takeshi.jdbc.esqlj.ConfigurationEnum;
import org.takeshi.jdbc.esqlj.elastic.model.ElasticField;
import org.takeshi.jdbc.esqlj.elastic.model.ElasticFieldExt;
import org.takeshi.jdbc.esqlj.elastic.model.ElasticFieldType;
import org.takeshi.jdbc.esqlj.elastic.model.IndexMetaData;
import org.takeshi.jdbc.esqlj.elastic.query.statement.SqlStatementSelect;
import org.takeshi.jdbc.esqlj.elastic.query.statement.model.Field;
import org.takeshi.jdbc.esqlj.support.EsRuntimeException;

public class RequestBuilderFields {
	
	public static void manageFields(SqlStatementSelect select, RequestInstance req) {
		getFieldsToRetrieve(select, req);
		addFieldsToRequest(select, req);
	}
	
	private static void getFieldsToRetrieve(SqlStatementSelect select, RequestInstance req) {		
		if(req.isStarSelect()) {
			req.setFields(req.getIndexMetaData().getFields());
			req.getFields().put(ElasticField.DOC_ID_ALIAS, getDocIdField());
		} else {
			req.setFields(select.getFields().stream().map(f -> ElasticFieldExt.promoteInstance(resolveField(req.getIndexMetaData(), f), f.getAlias())).collect(Collectors.toMap(ElasticFieldExt::getColumnName, Function.identity(), (o1, o2) -> o1, LinkedHashMap::new)));
		}
	}

	private static void addFieldsToRequest(SqlStatementSelect query, RequestInstance req) {
		List<String> sourceFields = !Configuration.getConfiguration(ConfigurationEnum.CFG_INCLUDE_TEXT_FIELDS_BY_DEFAULT, Boolean.class) && (query.getFields() == null || query.getFields().size() == 0) ?  null : new ArrayList<String>();
		req.getFields().forEach((name, field) -> {
			if(!field.isDocField()) {
				if(sourceFields != null) {
					sourceFields.add(field.getFullName());
				}
			} else if(!field.getFullName().equals(ElasticField.DOC_ID_ALIAS)) {
				req.getSearchSourceBuilder().docValueField(field.getFullName());
			}
		});
		
		if(sourceFields != null) {
			req.getSearchSourceBuilder().fetchSource(sourceFields.toArray(new String[sourceFields.size()]), null);
		}
	}
	
	private static ElasticField resolveField(IndexMetaData indexMetaData, Field f) throws RuntimeException {
		if(f.getName().equals(ElasticField.DOC_ID_ALIAS)) {
			return getDocIdField();
		}
		if(!indexMetaData.getFields().containsKey(f.getName())) {
			throw new EsRuntimeException(String.format("Unrecognized field %s", f.getName()));
		}
		return indexMetaData.getFields().get(f.getName());
	}
	
	
	private static ElasticField getDocIdField() {
		return new ElasticField(ElasticField.DOC_ID_ALIAS, ElasticFieldType.DOC_ID);
	}

}

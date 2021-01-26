package org.takeshi.jdbc.esqlj.elastic.query.impl.search;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.takeshi.jdbc.esqlj.Configuration;
import org.takeshi.jdbc.esqlj.ConfigurationEnum;
import org.takeshi.jdbc.esqlj.elastic.model.ElasticField;
import org.takeshi.jdbc.esqlj.elastic.model.ElasticFieldExt;
import org.takeshi.jdbc.esqlj.elastic.model.IndexMetaData;
import org.takeshi.jdbc.esqlj.parser.model.ParsedQuery;

public class RequestBuilder {

	public static RequestInstance buildRequest(IndexMetaData indexMetaData, ParsedQuery query, int fetchSize, boolean scrollable) {
		RequestInstance req = new RequestInstance(indexMetaData, fetchSize, scrollable, query);
		
		getFieldsToRetrieve(indexMetaData, query, req);
		addFieldsToRequest(indexMetaData, query, req);
		req.build();
		
		return req;
	}

	private static void getFieldsToRetrieve(IndexMetaData indexMetaData, ParsedQuery query, RequestInstance req) {		
		if(query.getFields() != null && query.getFields().size() > 0) {
			req.setFields(query.getFields().stream().map(f -> ElasticFieldExt.promoteInstance(indexMetaData.getFields().get(f.getName()), f.getAlias())).collect(Collectors.toMap(ElasticFieldExt::getColumnName, Function.identity(), (o1, o2) -> o1, LinkedHashMap::new)));
		} else {
			req.setFields(indexMetaData.getFields());
		}
	}
	
	private static void addFieldsToRequest(IndexMetaData indexMetaData, ParsedQuery query, RequestInstance req) {
		List<String> sourceFields = !Configuration.getConfiguration(ConfigurationEnum.CFG_INCLUDE_TEXT_FIELDS_BY_DEFAULT, Boolean.class) && (query.getFields() == null || query.getFields().size() == 0) ?  null : new ArrayList<String>();
		req.getFields().forEach((name, field) -> {
			if(!field.isDocField()) {
				if(sourceFields != null) {
					sourceFields.add(field.getFullName());
				}
			} else {
				req.getSearchSourceBuilder().docValueField(field.getFullName());
			}
		});
		
		if(sourceFields != null) {
			req.getSearchSourceBuilder().fetchSource(sourceFields.toArray(new String[sourceFields.size()]), null);
		}
	}
}

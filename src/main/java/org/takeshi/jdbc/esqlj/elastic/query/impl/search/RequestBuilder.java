package org.takeshi.jdbc.esqlj.elastic.query.impl.search;

import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.takeshi.jdbc.esqlj.Configuration;
import org.takeshi.jdbc.esqlj.ConfigurationEnum;
import org.takeshi.jdbc.esqlj.EsConnection;
import org.takeshi.jdbc.esqlj.elastic.model.ElasticField;
import org.takeshi.jdbc.esqlj.elastic.model.ElasticFieldExt;
import org.takeshi.jdbc.esqlj.elastic.model.ElasticFieldType;
import org.takeshi.jdbc.esqlj.elastic.model.IndexMetaData;
import org.takeshi.jdbc.esqlj.parser.model.Field;
import org.takeshi.jdbc.esqlj.parser.model.ParsedQuery;
import org.takeshi.jdbc.esqlj.support.ElasticUtils;
import org.takeshi.jdbc.esqlj.support.EsRuntimeException;

public class RequestBuilder {

	public static RequestInstance buildRequest(EsConnection connection, ParsedQuery query, int fetchSize) throws SQLException {
		RequestInstance req = new RequestInstance(connection, fetchSize, query);
		
		getFieldsToRetrieve(query, req);
		addFieldsToRequest(query, req);
		
		// to manage
		req.getSearchSourceBuilder().sort("amount", SortOrder.ASC);
		build(connection, req);
		
		return req;
	}

	private static void getFieldsToRetrieve(ParsedQuery query, RequestInstance req) {		
		if(req.isStarSelect()) {
			req.setFields(req.getIndexMetaData().getFields());
			req.getFields().put(ElasticField.DOC_ID_ALIAS, getDocIdField());
		} else {
			req.setFields(query.getFields().stream().map(f -> ElasticFieldExt.promoteInstance(resolveField(req.getIndexMetaData(), f), f.getAlias())).collect(Collectors.toMap(ElasticFieldExt::getColumnName, Function.identity(), (o1, o2) -> o1, LinkedHashMap::new)));
		}
	}

	private static void addFieldsToRequest(ParsedQuery query, RequestInstance req) {
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
	
	private static void build(EsConnection connection, RequestInstance req) throws SQLNonTransientConnectionException {
		req.getSearchSourceBuilder().size(req.getFetchSize()); 
		req.getSearchRequest().source(req.getSearchSourceBuilder());
		
		switch(req.getPaginationMode()) {
			case SCROLL_API:
				req.getSearchRequest().scroll(TimeValue.timeValueMinutes(Configuration.getConfiguration(ConfigurationEnum.CFG_QUERY_SCROLL_TIMEOUT_MINUTES, Long.class)));
				break;
			case BY_ORDER_WITH_PIT:
				req.getSearchRequest().setMaxConcurrentShardRequests(6); // enable work around
				PointInTimeBuilder pit = new PointInTimeBuilder(ElasticUtils.getPointInTime(connection, req));
				pit.setKeepAlive(TimeValue.timeValueMinutes(Configuration.getConfiguration(ConfigurationEnum.CFG_QUERY_SCROLL_TIMEOUT_MINUTES, Long.class)));
				req.getSearchSourceBuilder().pointInTimeBuilder(pit);
				break;
			default:
		}
	}
	
}

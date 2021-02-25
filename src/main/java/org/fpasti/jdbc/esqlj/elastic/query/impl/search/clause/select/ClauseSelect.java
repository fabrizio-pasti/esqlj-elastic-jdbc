package org.fpasti.jdbc.esqlj.elastic.query.impl.search.clause.select;

import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.fpasti.jdbc.esqlj.Configuration;
import org.fpasti.jdbc.esqlj.ConfigurationPropertyEnum;
import org.fpasti.jdbc.esqlj.elastic.model.ElasticFieldType;
import org.fpasti.jdbc.esqlj.elastic.model.ElasticObject;
import org.fpasti.jdbc.esqlj.elastic.model.IndexMetaData;
import org.fpasti.jdbc.esqlj.elastic.query.impl.search.RequestInstance;
import org.fpasti.jdbc.esqlj.elastic.query.statement.SqlStatementSelect;
import org.fpasti.jdbc.esqlj.elastic.query.statement.model.QueryColumn;
import org.fpasti.jdbc.esqlj.support.EsRuntimeException;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class ClauseSelect {
	
	public static void manageFields(SqlStatementSelect select, RequestInstance req) throws SQLSyntaxErrorException {
		getFieldsToRetrieve(select, req);
		
		switch(select.getQueryType()) {
			case DOCS:
				addFieldsToRequest(select, req);
				break;
			default:
				AggregationBuilder.doAggregation(req);
				break;
		}
	}

	private static void getFieldsToRetrieve(SqlStatementSelect select, RequestInstance req) {		
		if(req.isStarSelect()) {
			req.setFields(req.getIndexMetaData().getElasticObjects());
			req.getFields().put(ElasticObject.DOC_ID_ALIAS, getDocIdField());
			req.getFields().put(ElasticObject.DOC_SCORE, getDocScoreField());
		} else {
			req.setFields(select.getQueryColumns().stream().map(f -> resolveField(req.getIndexMetaData(), f)).collect(Collectors.toMap(ElasticObject::getColumnName, Function.identity(), (o1, o2) -> o1, LinkedHashMap::new)));
			req.setColumnNames(select.getQueryColumns().stream().map(field -> field.getName()).collect(Collectors.toList()));
		}
	}

	private static void addFieldsToRequest(SqlStatementSelect query, RequestInstance req) {
		List<String> sourceFields = !Configuration.getConfiguration(ConfigurationPropertyEnum.CFG_INCLUDE_TEXT_FIELDS_BY_DEFAULT, Boolean.class) && (query.getQueryColumns() == null || query.getQueryColumns().size() == 0) ?  null : new ArrayList<String>();
		req.getFields().forEach((name, field) -> {
			if(!field.isDocValue()) {
				if(sourceFields != null) {
					sourceFields.add(field.getFullName());
				}
			} else if(!field.getFullName().equals(ElasticObject.DOC_ID_ALIAS)) {
				req.getSearchSourceBuilder().docValueField(field.getFullName());
			} 
		});
		
		if(sourceFields != null) {
			req.getSearchSourceBuilder().fetchSource(sourceFields.toArray(new String[sourceFields.size()]), null);
		}
	}
	
	private static ElasticObject resolveField(IndexMetaData indexMetaData, QueryColumn queryColumn) throws RuntimeException {
		if(queryColumn.getName().equals(ElasticObject.DOC_ID_ALIAS)) {
			return getDocIdField();
		}
		if(queryColumn.getName().equals(ElasticObject.DOC_SCORE)) {
			return getDocScoreField();
		}
		if(queryColumn.getAggregatingFunctionExpression() != null) {
			return getFunctionField(queryColumn);
		}
		if(!indexMetaData.getElasticObjects().containsKey(queryColumn.getName()) && queryColumn.getFormatter() == null) {
			throw new EsRuntimeException(String.format("Unrecognized field %s", queryColumn.getName()));
		}
		
		ElasticObject elObj = indexMetaData.getElasticObjects().get(queryColumn.getName()).clone();
		elObj.setLinkedQueryColumn(queryColumn);
		return elObj;
	}
	
	private static ElasticObject getDocIdField() {
		return new ElasticObject(ElasticObject.DOC_ID_ALIAS, ElasticFieldType.DOC_ID);
	}
	
	private static ElasticObject getDocScoreField() {
		return new ElasticObject(ElasticObject.DOC_SCORE, ElasticFieldType.DOC_SCORE);
	}

	private static ElasticObject getFunctionField(QueryColumn f) {
		return new ElasticObject(f);
	}
}

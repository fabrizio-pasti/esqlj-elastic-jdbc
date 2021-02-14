package org.fpasti.jdbc.esqlj.elastic.query.impl.search.clause.select;

import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.fpasti.jdbc.esqlj.Configuration;
import org.fpasti.jdbc.esqlj.ConfigurationPropertyEnum;
import org.fpasti.jdbc.esqlj.elastic.model.ElasticField;
import org.fpasti.jdbc.esqlj.elastic.model.ElasticFieldExt;
import org.fpasti.jdbc.esqlj.elastic.model.ElasticFieldType;
import org.fpasti.jdbc.esqlj.elastic.model.IndexMetaData;
import org.fpasti.jdbc.esqlj.elastic.query.impl.search.RequestInstance;
import org.fpasti.jdbc.esqlj.elastic.query.statement.SqlStatementSelect;
import org.fpasti.jdbc.esqlj.elastic.query.statement.model.QueryColumn;
import org.fpasti.jdbc.esqlj.elastic.query.statement.model.QueryType;
import org.fpasti.jdbc.esqlj.support.EsRuntimeException;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class ClauseSelect {
	
	public static void manageFields(SqlStatementSelect select, RequestInstance req) throws SQLSyntaxErrorException {
		getFieldsToRetrieve(select, req);
		if(select.getQueryType().equals(QueryType.DOCS)) {
			addFieldsToRequest(select, req);
		} else {
			AggregationBuilder.doAggregation(select, req);
		}
	}

	private static void getFieldsToRetrieve(SqlStatementSelect select, RequestInstance req) {		
		if(req.isStarSelect()) {
			req.setFields(req.getIndexMetaData().getFields());
			req.getFields().put(ElasticField.DOC_ID_ALIAS, getDocIdField());
			req.getFields().put(ElasticField.DOC_SCORE, getDocScoreField());
		} else {
			req.setFields(select.getFields().stream().map(f -> {
				ElasticField field = resolveField(req.getIndexMetaData(), f);
				if(field instanceof ElasticFieldExt) {
					return (ElasticFieldExt)field;
				}
				return ElasticFieldExt.promoteInstance(field, f);
				}).collect(Collectors.toMap(ElasticFieldExt::getColumnName, Function.identity(), (o1, o2) -> o1, LinkedHashMap::new)));
			req.setColumnNames(select.getFields().stream().map(field -> field.getName()).collect(Collectors.toList()));
		}
	}

	private static void addFieldsToRequest(SqlStatementSelect query, RequestInstance req) {
		List<String> sourceFields = !Configuration.getConfiguration(ConfigurationPropertyEnum.CFG_INCLUDE_TEXT_FIELDS_BY_DEFAULT, Boolean.class) && (query.getFields() == null || query.getFields().size() == 0) ?  null : new ArrayList<String>();
		req.getFields().forEach((name, field) -> {
			if(!field.isDocValue()) {
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
	
	private static ElasticField resolveField(IndexMetaData indexMetaData, QueryColumn f) throws RuntimeException {
		if(f.getName().equals(ElasticField.DOC_ID_ALIAS)) {
			return getDocIdField();
		}
		if(f.getName().equals(ElasticField.DOC_SCORE)) {
			return getDocScoreField();
		}
		if(f.getAggregatingFunction() != null) {
			return getFunctionField(f);
		}
		if(!indexMetaData.getFields().containsKey(f.getName()) && f.getFormatter() == null) {
			throw new EsRuntimeException(String.format("Unrecognized field %s", f.getName()));
		}
		return indexMetaData.getFields().get(f.getName());
	}
	
	private static ElasticField getDocIdField() {
		return new ElasticFieldExt(ElasticField.DOC_ID_ALIAS, ElasticFieldType.DOC_ID);
	}
	
	private static ElasticField getDocScoreField() {
		return new ElasticFieldExt(ElasticField.DOC_SCORE, ElasticFieldType.DOC_SCORE);
	}

	private static ElasticField getFunctionField(QueryColumn f) {
		return new ElasticFieldExt(f);
	}
}

package org.fpasti.jdbc.esqlj.elastic.query.impl.search;

import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.fpasti.jdbc.esqlj.Configuration;
import org.fpasti.jdbc.esqlj.ConfigurationPropertyEnum;
import org.fpasti.jdbc.esqlj.EsConnection;
import org.fpasti.jdbc.esqlj.elastic.query.impl.search.clause.ClauseSort;
import org.fpasti.jdbc.esqlj.elastic.query.impl.search.clause.ClauseWhere;
import org.fpasti.jdbc.esqlj.elastic.query.impl.search.clause.select.ClauseSelect;
import org.fpasti.jdbc.esqlj.elastic.query.statement.SqlStatementSelect;
import org.fpasti.jdbc.esqlj.elastic.query.statement.model.QueryType;
import org.fpasti.jdbc.esqlj.support.ElasticUtils;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class RequestBuilder {

	public static RequestInstance buildRequest(EsConnection connection, SqlStatementSelect select, int fetchSize) throws SQLException {
		RequestInstance req = new RequestInstance(connection, fetchSize, select);
		
		ClauseSelect.manageFields(select, req);
		ClauseSort.manageSort(select, req);
		ClauseWhere.manageWhere(select, req);
		
		build(connection, req, select);
		
		return req;
	}

	private static void build(EsConnection connection, RequestInstance req, SqlStatementSelect select) throws SQLNonTransientConnectionException {
		if(select.getQueryType().equals(QueryType.DOCS)) {
			req.getSearchSourceBuilder().size(select.getLimit() != null ? (select.getLimit() > req.getFetchSize() ? req.getFetchSize() : select.getLimit().intValue()) : req.getFetchSize());
		}
		
		req.getSearchRequest().source(req.getSearchSourceBuilder());
		
		switch(req.getPaginationMode()) {
			case SCROLL_API:
				req.getSearchRequest().scroll(TimeValue.timeValueMinutes(Configuration.getConfiguration(ConfigurationPropertyEnum.CFG_QUERY_SCROLL_TIMEOUT_MINUTES, Long.class)));
				break;
			case BY_ORDER_WITH_PIT:
				req.getSearchRequest().setMaxConcurrentShardRequests(6); // enable work around
				PointInTimeBuilder pit = new PointInTimeBuilder(ElasticUtils.getPointInTime(connection, req));
				pit.setKeepAlive(TimeValue.timeValueMinutes(Configuration.getConfiguration(ConfigurationPropertyEnum.CFG_QUERY_SCROLL_TIMEOUT_MINUTES, Long.class)));
				req.getSearchSourceBuilder().pointInTimeBuilder(pit);
				break;
			default:
		}
	}
	
}

package org.takeshi.jdbc.esqlj.support;

import java.io.IOException;
import java.sql.SQLNonTransientConnectionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.ParseException;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.takeshi.jdbc.esqlj.Configuration;
import org.takeshi.jdbc.esqlj.ConfigurationEnum;
import org.takeshi.jdbc.esqlj.EsConnection;
import org.takeshi.jdbc.esqlj.elastic.query.impl.search.RequestInstance;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class ElasticUtils {

	public static String getPointInTime(EsConnection connection, RequestInstance req) throws SQLNonTransientConnectionException  {
		try {
			Request rawReq = new Request("POST", String.format("/%s/_pit?keep_alive=%dm", req.getIndexMetaData().getIndex(), Configuration.getConfiguration(ConfigurationEnum.CFG_QUERY_SCROLL_TIMEOUT_MINUTES, Long.class)));
			Response res = connection.getElasticClient().getLowLevelClient().performRequest(rawReq);
			Pattern pattern = Pattern.compile("\"id\":\\s*\"([\\w=]*)\"");
		    Matcher matcher = pattern.matcher(EntityUtils.toString(res.getEntity()));
		    return matcher.group(1);
		} catch(ParseException | IOException e) {
			throw new SQLNonTransientConnectionException(e);
		}
	}
	
	public static void deletePointInTime(EsConnection connection, String pit) {
		try {
			Request rawReq = new Request("DELETE", "/_pit");
		    rawReq.setJsonEntity(String.format("{\"id\" : \"%s\"}", pit));
		    connection.getElasticClient().getLowLevelClient().performRequest(rawReq);			
		} catch(ParseException | IOException e) {
			// nop
		}		
	}

	
}

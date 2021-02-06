package org.takeshi.jdbc.esqlj;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Connection;
import java.sql.SQLException;

import org.fpasti.jdbc.esqlj.Configuration;
import org.fpasti.jdbc.esqlj.support.ElasticInstance.HttpProtocol;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.takeshi.jdbc.esqlj.testUtils.TestUtils;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class ConnectionTestUnit {

	@ParameterizedTest(name = "{1} = {0}")
	@CsvSource(delimiter = ';', value = {
		"true; jdbc:esqlj:http://10.53.137.170:9200",
		"true; jdbc:esqlj:http://elastic.server.com:9200",
		"true; jdbc:esqlj:https://elastic.server.com",
		"true; jdbc:esqlj:https://10.53.137.170:9200",
		"true; jdbc:esqlj:http://10.53.137.170:9200,http://10.53.137.171:9200",
		"true; jdbc:esqlj:http://10.53.137.170:9200,http://elastic.server.com:9200",
		"true; jdbc:esqlj:http://10.53.137.170",
		"true; jdbc:esqlj:http://elastic.server.com",
		"false; jdb:esqlj:http://10.53.137.170:9200",
		"false; jdb:esql:http://10.53.137.170:9200",
		"false; jdbc:esqlj:htt://:9200",
		"false; jdbc:esqlj:http:/10.53.137.170:9200",
		"false; jdbc:esqlj:http//10.53.137.170:9200",
		"false; jdbc:esqlj://10.53.137.170:9200",
		"false; jdbc:esqlj://10.53.137.170",
		"false; jdbc:esqlj://10.53.137.170:9200",
		"false; jdbc:esqlj:http://10.53.137.170:9200,elastic.server.com:9200"
		})
	public void checkUrl(boolean valid, String url) throws SQLException {
		if (valid) {
			Connection connection = TestUtils.getTestConnection(url, null);
			assertNotNull(connection);
		} else {
			SQLException e = assertThrows(SQLException.class, () -> {
				TestUtils.getTestConnection(url, null);
			});
			assertEquals(e.getMessage(), "Invalid connection string");
		}
	}

	@ParameterizedTest
	@CsvSource({
	"jdbc:esqlj:http://10.53.137.170:9200, http, 10.53.137.170, 9200",
	"jdbc:esqlj:http://10.53.137.170, http, 10.53.137.170, 9200",
	"jdbc:esqlj:https://elastic.server.com:9201, https, elastic.server.com, 9201"
	})
	public void checkUrl2(String url, String protocol, String server, int port) throws SQLException {
		HttpProtocol httpProtocol = HttpProtocol.https;;
		if(protocol.equals("http")) {
			httpProtocol = HttpProtocol.http;
		} 
		TestUtils.getTestConnection(url, null);
		assertEquals(Configuration.getUrls().get(0).getProtocol(), httpProtocol);
		assertEquals(Configuration.getUrls().get(0).getServer(), server);
		assertEquals(Configuration.getUrls().get(0).getPort(), port);
	}
}

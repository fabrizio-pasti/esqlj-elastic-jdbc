package org.fpasti.jdbc.esqlj;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.fpasti.jdbc.esqlj.support.ElasticInstance.HttpProtocol;
import org.fpasti.jdbc.esqlj.testUtils.TestUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class TestConnection {

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
		Properties properties = new Properties();
		properties.put(ConfigurationPropertyEnum.CFG_SHARED_CONNECTION.getConfigName(), false);
		if (valid) {
			Connection connection = TestUtils.getTestConnection(url, properties);
			assertNotNull(connection);
		} else {
			SQLException e = assertThrows(SQLException.class, () -> {
				TestUtils.getTestConnection(url, properties);
			});
			assertEquals("Invalid connection string", e.getMessage());
		}
	}

	@ParameterizedTest
	@CsvSource({
	"jdbc:esqlj:http://10.53.137.170:9200, http, 10.53.137.170, 9200",
	"jdbc:esqlj:http://10.53.137.170, http, 10.53.137.170, 9200",
	"jdbc:esqlj:https://elastic.server.com:9201, https, elastic.server.com, 9201"
	})
	public void checkUrl2(String url, String protocol, String server, int port) throws SQLException {
		HttpProtocol httpProtocol = HttpProtocol.https;
		Properties properties = new Properties();
		properties.put(ConfigurationPropertyEnum.CFG_SHARED_CONNECTION.getConfigName(), false);
		if(protocol.equals("http")) {
			httpProtocol = HttpProtocol.http;
		} 
		TestUtils.getTestConnection(url, properties);
		assertEquals(httpProtocol, Configuration.getUrls().get(0).getProtocol());
		assertEquals(server, Configuration.getUrls().get(0).getServer());
		assertEquals(port, Configuration.getUrls().get(0).getPort());
	}
}

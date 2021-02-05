package org.takeshi.jdbc.esqlj;

import static junitparams.JUnitParamsRunner.$;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.takeshi.jdbc.esqlj.support.ElasticInstance.HttpProtocol;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

@RunWith(JUnitParamsRunner.class)
public class ConnectionTestUnit {

	@SuppressWarnings({ "unused", "deprecation" })
	private static final Object[] urls() {
		return $($(true, "jdbc:esqlj:http://10.53.137.170:9200"), $(true, "jdbc:esqlj:http://elastic.server.com:9200"),
				$(true, "jdbc:esqlj:https://elastic.server.com"), $(true, "jdbc:esqlj:https://10.53.137.170:9200"),
				$(true, "jdbc:esqlj:http://10.53.137.170:9200,http://10.53.137.171:9200"),
				$(true, "jdbc:esqlj:http://10.53.137.170:9200,http://elastic.server.com:9200"),
				$(true, "jdbc:esqlj:http://10.53.137.170"), $(true, "jdbc:esqlj:http://elastic.server.com"),
				$(false, "jdb:esqlj:http://10.53.137.170:9200"), $(false, "jdb:esql:http://10.53.137.170:9200"),
				$(false, "jdbc:esqlj:htt://:9200"), $(false, "jdbc:esqlj:http:/10.53.137.170:9200"),
				$(false, "jdbc:esqlj:http//10.53.137.170:9200"), $(false, "jdbc:esqlj://10.53.137.170:9200"),
				$(false, "jdbc:esqlj://10.53.137.170"), $(false, "jdbc:esqlj://10.53.137.170:9200"),
				$(false, "jdbc:esqlj:http://10.53.137.170:9200,//elastic.server.com:9200"));
	}
	
	@SuppressWarnings({ "unused", "deprecation" })
	private static final Object[] urls2() {
		return $(
				$("jdbc:esqlj:http://10.53.137.170:9200", HttpProtocol.http, "10.53.137.170", 9200),
				$("jdbc:esqlj:http://10.53.137.170", HttpProtocol.http, "10.53.137.170", 9200),
				$("jdbc:esqlj:https://elastic.server.com:9201", HttpProtocol.https, "elastic.server.com", 9201)
			);
	}

	@Test
	@Parameters(method = "urls")
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

	@Test
	@Parameters(method = "urls2")
	public void checkUrl2(String url, HttpProtocol protocol, String server, int port) throws SQLException {
		TestUtils.getTestConnection(url, null);
		assertEquals(Configuration.getUrls().get(0).getProtocol(), protocol);
		assertEquals(Configuration.getUrls().get(0).getServer(), server);
		assertEquals(Configuration.getUrls().get(0).getPort(), port);
	}
}

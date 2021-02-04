package org.takeshi.jdbc.esqlj;

import static junitparams.JUnitParamsRunner.$;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

@RunWith(JUnitParamsRunner.class)
public class MetadataTestUnit
{

    @SuppressWarnings({ "unused", "deprecation" })
	private static final Object[] urls () {
        return $(
        			$("jdbc:esqlj:http://153.77.137.170:9200", "http://153.77.137.170:9200"),
        			$("jdbc:esqlj:http://153.77.137.170", "http://153.77.137.170:9200"),
        			$("jdbc:esqlj:http://elastic.server.com:9200", "http://elastic.server.com:9200"),
        			$("jdbc:esqlj:https://153.77.137.170:9200", "https://153.77.137.170:9200"),
        			$("jdbc:esqlj:http://153.77.137.170:9200,http://153.77.137.171:9200", "http://153.77.137.170:9200,http://153.77.137.171:9200")
                );
    }
    
    @Test
	@Parameters(method = "urls")
	public void checkUrl(String url, String checkUrl) throws SQLException {
		Connection connection = TestUtils.getTestConnection(url, null);
		assertEquals(connection.getMetaData().getURL(), checkUrl);
	}
	
}

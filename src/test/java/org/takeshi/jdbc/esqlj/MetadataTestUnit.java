package org.takeshi.jdbc.esqlj;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.takeshi.jdbc.esqlj.test.ElasticLiveEnvironment;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

@ExtendWith(ElasticLiveEnvironment.class)
public class MetadataTestUnit
{

	@Test
	public void isPresent() throws SQLException {
		System.out.println("metadata test unit");
		assertNotNull(new Object());
		
	}
 
    /*@SuppressWarnings({ "unused", "deprecation" })
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
	}*/
	
}

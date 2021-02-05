package org.takeshi.jdbc.esqlj;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.fpasti.jdbc.esqlj.ConfigurationPropertyEnum;
import org.fpasti.jdbc.esqlj.EsConnection;


/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class TestUtils {
	private static String TEST_CONNECTION_STRING = "ESQLJ_CONNECTION_STRING";
	
    private TestUtils() {
    }

    private static String getProperty(String key) {
    	String val = System.getenv(key);
    	if(val == null) {
    		System.err.println(String.format("Undeclared system property : %s", key));
    	}
    	return val;
    }
    
    public static EsConnection getLiveConnection(Properties info) throws SQLException {
        return (EsConnection) DriverManager.getConnection(getProperty(TEST_CONNECTION_STRING), info);
    }

    public static EsConnection getTestConnection(String url, Properties info) throws SQLException {
    	if(info == null) {
    		info = new Properties();
    	}
    	info.put(ConfigurationPropertyEnum.CFG_TEST_MODE.name, true);
        return (EsConnection) DriverManager.getConnection(url, info);
    }

}
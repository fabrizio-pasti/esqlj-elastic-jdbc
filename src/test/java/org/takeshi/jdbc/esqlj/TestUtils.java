package org.takeshi.jdbc.esqlj;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.fpasti.jdbc.esqlj.ConfigurationPropertyEnum;
import org.fpasti.jdbc.esqlj.EsConnection;
import org.fpasti.jdbc.esqlj.EsDriver;


/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class TestUtils {
	private static String ENV_PROP_ESQLJ_TEST_CONFIG = "ESQLJ_TEST_CONFIG";
	
	static {
		try {
			DriverManager.registerDriver(new EsDriver());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
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
    	String testConfig = getProperty(ENV_PROP_ESQLJ_TEST_CONFIG);
    	if(testConfig == null) {
    		return null;
    	}
    	DriverManager.registerDriver(new EsDriver());
    	String[] connectionUrl = testConfig.split("|");
    	String indexTestingMode = connectionUrl[1]; 
        return (EsConnection) DriverManager.getConnection(connectionUrl[0], info);
    }

    public static EsConnection getTestConnection(String url, Properties info) throws SQLException {
    	if(info == null) {
    		info = new Properties();
    	}
    	info.put(ConfigurationPropertyEnum.CFG_TEST_MODE.name, true);
        return (EsConnection) DriverManager.getConnection(url, info);
    }

}
package org.takeshi.jdbc.esqlj.testUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.fpasti.jdbc.esqlj.ConfigurationPropertyEnum;
import org.fpasti.jdbc.esqlj.EsConnection;
import org.fpasti.jdbc.esqlj.EsDriver;


/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class TestUtils {
	
	public static final String ENV_PROP_ESQLJ_TEST_CONFIG = "ESQLJ_TEST_CONFIG";
	private static final String SETUP_MODE_CREATE_AND_DESTROY = "createAndDestroy	"; 
	private static final String SETUP_MODE_CREATE_ONLY = "createOnly";
	
	private static EsConnection connection;
	private static String[] systemProperties;
	
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
    
    public static EsConnection getLiveConnection() throws SQLException {
    	return getLiveConnection(null);
    }
    
    public static EsConnection getLiveConnection(Properties info) throws SQLException {
    	if(connection != null) {
    		return connection;
    	}
    	String testConfig = getProperty(ENV_PROP_ESQLJ_TEST_CONFIG);
    	if(testConfig == null) {
    		return null;
    	}
    	DriverManager.registerDriver(new EsDriver());
    	systemProperties = testConfig.split("\\|"); 
    	connection = (EsConnection) DriverManager.getConnection(systemProperties[0], info);
        return connection;
    }

    public static EsConnection getTestConnection(String url, Properties info) throws SQLException {
    	if(info == null) {
    		info = new Properties();
    	}
    	info.put(ConfigurationPropertyEnum.CFG_TEST_MODE.name, true);
        return (EsConnection) DriverManager.getConnection(url, info);
    }

	public static void setupElastic() throws Exception {
		Properties properties = new Properties();
		properties.put(ConfigurationPropertyEnum.CFG_SHARED_CONNECTION.getConfigName(), false);
		EsConnection connection = getLiveConnection(properties);
		
		switch(systemProperties[1]) {
			case SETUP_MODE_CREATE_AND_DESTROY:
				ElasticTestService.setup(connection, true);
				break;
			case SETUP_MODE_CREATE_ONLY:
				ElasticTestService.setup(connection, false);
				break;
			default:
				System.out.println(String.format("Unsupported option '%s'", systemProperties[1]));
				break;
		}
	}
	
	public static void tearOffElastic() throws SQLException, IOException {
		switch(systemProperties[1]) {
			case SETUP_MODE_CREATE_AND_DESTROY:
				ElasticTestService.tearOff(getLiveConnection(null));
				break;
		}
	}
	
	public static String getResourceAsText(String filePath) {
		InputStream inputStream = TestUtils.class.getClassLoader().getResourceAsStream(filePath);
		return new BufferedReader(
			      new InputStreamReader(inputStream, StandardCharsets.UTF_8))
			        .lines()
			        .collect(Collectors.joining("\n"));
	}
	
	public static File[] listFiles(String path) {
		URL pathUrl = TestUtils.class.getClassLoader().getResource(path);
	    return new File(pathUrl.getPath()).listFiles();
	}
	
	public static String readFile(File file) throws IOException {
        StringBuilder contentBuilder = new StringBuilder();
        try (Stream<String> stream = Files.lines(Paths.get(file.getPath()), StandardCharsets.UTF_8)) {
			stream.forEach(s -> contentBuilder.append(s).append("\n"));
		} 
        return contentBuilder.toString();
	}
	
	public static String getURL() {
		return systemProperties[0];
	}

	public static String resolveTestIndex(String txt) {
		return txt.replace("testIndexStar", "\"".concat(ElasticTestService.CURRENT_INDEX).concat("*\"")).replace("testIndex", "\"".concat(ElasticTestService.CURRENT_INDEX).concat("\""));
	}

}
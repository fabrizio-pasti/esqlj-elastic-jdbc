package org.takeshi.jdbc.esqlj;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.takeshi.jdbc.esqlj.support.ElasticInstance;
import org.takeshi.jdbc.esqlj.support.ElasticInstance.HttpProtocol;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class Configuration {
	
	static {
		init();
    }
	
	private static int MINOR_VERSION = 0;
	private static int MAJOR_VERSION = 1;
	
	private static Map<ConfigurationPropertyEnum, Object> properties;
	private static String connectionString;
	private static List<ElasticInstance> urls;
	
	public static int getMinorVersion() {
		return MINOR_VERSION;
	}
	
	public static int getMajorVersion() {
		return MAJOR_VERSION;
	}
	
	public static String getConnectionString() {
		return connectionString;
	}
	
	public static List<ElasticInstance> getUrls() {
		return urls;
	}
	
	@SuppressWarnings("unchecked")
	public static <T> T getConfiguration(ConfigurationPropertyEnum id, Class<T> clazz) {
	    return (T)properties.get(id);
	} 

	public boolean isLoginRequired() {
		return properties.get(ConfigurationPropertyEnum.CFG_USERNAME) != null && properties.get(ConfigurationPropertyEnum.CFG_PASSWORD) != null; 
	}

	private static void init() {
		properties = new HashMap<ConfigurationPropertyEnum, Object>();
		Arrays.stream(ConfigurationPropertyEnum.values()).forEach(prop -> properties.put(prop, prop.defaultValue));		
	}
		
	public static void parseConnectionString(String connectionString, Properties info) throws SQLException {
		String params = "";
		urls = new ArrayList<ElasticInstance>();
		String sUrls;
		
		try {
			sUrls = connectionString.substring(11);
			if(sUrls.contains(";")) {
				params = sUrls.substring(sUrls.indexOf(";"));
				params = params.length() == 1 ? "" : params.substring(1);
				sUrls = sUrls.substring(0, sUrls.indexOf(";"));
			}
			
			resolveServerInstances(sUrls);
			resolveConnectionProperties(params);
			digestProperties(info);
			
		} catch(SQLException se) {
			 throw se;
		} catch(Exception e) {
			throw new SQLException("Invalid connection string");
		}
	}

	private static void resolveServerInstances(String sUrls) {
		Pattern pattern = Pattern.compile("(^.*?):\\/\\/([^:\\/\\s]+):?(\\d*)?");
		for(String sUrl : sUrls.split(",")) {
			Matcher matcher = pattern.matcher(sUrl);
			matcher.find();
			urls.add(new ElasticInstance(HttpProtocol.valueOf(matcher.group(1)), matcher.group(2), matcher.group(3).length() == 0 ? 9200 : Integer.parseInt(matcher.group(3))));
		}
	}

	private static void resolveConnectionProperties(String params) throws SQLException {
		String[] keyVals = params.split(";");
		for(String keyVal : keyVals)
		{
		  String[] kv = keyVal.split("=", 2);
		  if(kv[0].length() == 0 || kv.length == 1) {
			  continue;
		  }
		  if(ConfigurationPropertyEnum.fromName(kv[0]) == null) {
			  throw new SQLException(String.format("Invalid connection string. Unknown property: %s",  kv[0]));
		  }
		  properties.put(ConfigurationPropertyEnum.fromName(kv[0]), castString(kv[1], ConfigurationPropertyEnum.fromName(kv[0]).clazz));
		}
	}

	private static void digestProperties(Properties info) throws SQLException {
		if(info == null) {
			return;
		}
		
		for(Object key : info.keySet()) {
			Object value = info.get(key);
			if(ConfigurationPropertyEnum.fromName(key.toString()) == null) {
				throw new SQLException(String.format("Invalid connection string. Unknown property: %s",  key.toString()));
			}
			properties.put(ConfigurationPropertyEnum.fromName(key.toString()), value);
		}
	}
	
	public static DriverPropertyInfo[] getDriverPropertyInfo() {
        List<DriverPropertyInfo> propertiesInfo = new ArrayList<DriverPropertyInfo>();
        properties.forEach((key, value) -> {
        	DriverPropertyInfo dp = new DriverPropertyInfo(key.name, value.toString());
        	dp.choices = key.options;
        	dp.description = key.description;
        	dp.required = key.required;
        	propertiesInfo.add(dp);
        });
        
        return propertiesInfo.toArray(new DriverPropertyInfo[propertiesInfo.size()]);
    }

	public static String getUrl() {
		return urls.stream().map(ei -> ei.getUrl()).reduce("", (a, b) -> a.concat(a.length() == 0 ? "" : ",").concat(b));
	}

	public static boolean isTestMode() {
		return Configuration.getConfiguration(ConfigurationPropertyEnum.CFG_TEST_MODE, Boolean.class);
	}
	
	private static Object castString(String value, Class<?> clazz) {
		switch(clazz.getSimpleName()) {
			case "Integer":
				return Integer.parseInt(value);
			case "Long":
				return Long.parseLong(value);
			case "Boolean":
				return Boolean.parseBoolean(value);
			default:
				return value;
		}
	}
}

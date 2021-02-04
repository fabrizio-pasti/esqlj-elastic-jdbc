package org.takeshi.jdbc.esqlj;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class Configuration {

	private static Map<ConfigurationEnum, Object> configuration = new HashMap<ConfigurationEnum, Object>();
	
	static {
		Arrays.asList(ConfigurationEnum.values()).forEach(config -> {
			configuration.put(config, config.getDefaultValue());
		});
	}

	public static void parseConnectionString(String connectionString) {
		
	}
	
	@SuppressWarnings("unchecked")
	public static <T> T getConfiguration(ConfigurationEnum config, Class<T> clazz) {
		return (T) configuration.get(config);
	}
}

package org.fpasti.jdbc.esqlj;

import java.util.Arrays;

public enum ConfigurationPropertyEnum {
		CFG_USERNAME("userName", String.class, null, null, false, "User name"),
		CFG_PASSWORD("password", String.class, null, null, false, "Password"),
		CFG_TEST_MODE("testMode", Boolean.class, false, null, false, "Test mode"),
		CFG_INCLUDE_TEXT_FIELDS_BY_DEFAULT("includeTextFieldsByDefault", Boolean.class, false, null, false, "Include text fields by default"),
		CFG_INDEX_METADATA_CACHE("indexMetaDataCache", Boolean.class, false, null, false, "Index metadata cache"),
		CFG_QUERY_SCROLL_FETCH_SIZE("queryScrollFetchSize", Integer.class, 500, null, false, "Query scroll fetch size"),
		CFG_QUERY_SCROLL_TIMEOUT_MINUTES("queryScrollTimeoutMinutes", Long.class, 3L, null, false, "Query scroll timeout expressed in minutes"),
		CFG_QUERY_SCROLL_FROM_ROWS("queryScrollFromRows", Long.class, 500L, null, false, "Number of rows before scroll"),
		CFG_QUERY_SCROLL_ONLY_BY_SCROLL_API("queryScrollOnlyByScrollApi", Boolean.class, true, null, false, "Scroll using Elastic Scroll API"),
		CFG_SHARED_CONNECTION("sharedConnection", Boolean.class, true, null, false, "Native Elastic connection shared between JDBC connection");
		
		public String name;
		public Class<?> clazz;
		public Object defaultValue;
		public String[] options;
		public boolean required;
		public String description;
		
		private ConfigurationPropertyEnum(String name, Class<?> clazz, Object defaultValue, String[] options, boolean required, String description) {
			this.name = name;
			this.clazz = clazz;
			this.defaultValue = defaultValue;
			this.options = options;
			this.required = required;
			this.description = description;
		}
		
		public static ConfigurationPropertyEnum fromName(String name) {
	        return Arrays.stream(values()).filter(e -> e.name.equals(name)).findFirst().orElse(null);
	    }
		
	}
package org.takeshi.jdbc.esqlj;

import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;

public enum ConfigurationEnum {

		CFG_INCLUDE_TEXT_FIELDS_BY_DEFAULT("includeTextFieldsByDefault", Boolean.class, false),
		CFG_INDEX_METADATA_CACHE("indexMetaDataCache", Boolean.class, false),
		CFG_QUERY_FETCH_SIZE("queryFetchSize", Integer.class, 100),
		CFG_QUERY_SCROLL_TIMEOUT_MINUTES("queryScrollTimeoutMinutes", Long.class, 1L);
		
		String name;
		Class<?> clazz;
		Object defaultValue;

		ConfigurationEnum(String name, Class<?> clazz, Object defaultValue) {
			this.name = name;
			this.clazz = clazz;
			this.defaultValue = defaultValue;
		}
		
		public String getName() {
			return name;
		}
		
		@SuppressWarnings("rawtypes")
		public Class getClazz() {
			return clazz;
		}

		public Object getDefaultValue() {
			return defaultValue;
		}

		public static ConfigurationEnum resolveByName(String name) {
			return Arrays.stream(ConfigurationEnum.values()).filter(elt -> elt.name.equals(name)).findFirst()
					.orElseGet(null);
		}
		
	}
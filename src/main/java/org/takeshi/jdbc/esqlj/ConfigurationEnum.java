package org.takeshi.jdbc.esqlj;

import java.util.Arrays;

public enum ConfigurationEnum {

		CFG_INCLUDE_TEXT_FIELDS_BY_DEFAULT("includeTextFieldsByDefault", Boolean.class, true),
		CFG_INDEX_METADATA_CACHE("indexMetaDataCache", Boolean.class, false),
		CFG_QUERY_SCROLL_FETCH_SIZE("queryScrollFetchSize", Integer.class, 100),
		CFG_QUERY_SCROLL_TIMEOUT_MINUTES("queryScrollTimeoutMinutes", Long.class, 1L),
		CFG_QUERY_SCROLL_FROM_ROWS("queryScrollFromRows", Long.class, 100L),
		CFG_QUERY_SCROLL_ONLY_BY_SCROLL_API("queryScrollOnlyByScrollApi", Boolean.class, true);
		
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
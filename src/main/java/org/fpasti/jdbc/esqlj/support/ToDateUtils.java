package org.fpasti.jdbc.esqlj.support;

import java.sql.SQLSyntaxErrorException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class ToDateUtils {
	 
	private enum FormatMapper {
		YEAR("yyyy"),
		YYYY("yyyy"),
		YY("yy"),
		MM("MM"),
		MONTH("MMMM"),
		MON("MM"),
		DDD("D_D_D"),
		DD("dd"),
		HH24("H_H"),
		HH12("hh"),
		HH("hh"),
		MI("mm"),
		SS("ss"),
		DAY("EEE"),
		AD("G"),
		XFF(".S_S_S"),
		FFF("S_S_S"),
		FF("S_S"),
		F("S"),
		PM("a"),
		AM("a"),
		TZR("z"),
		TZH("X");
		
		
		private String javaFormat;
		
		FormatMapper(String javaFormat) {
			this.javaFormat = javaFormat;
		}
		
		public String getJavaFormat() {
			return javaFormat;
		}
	}
	
	public static LocalDateTime resolveToDate(String date, String mask) throws SQLSyntaxErrorException {
		String format = convertToJavaFormat(mask); 
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		try {
			return sdf.parse(date).toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
		} catch (ParseException e) {
			throw new SQLSyntaxErrorException(String.format("Failed to parse date '%s' with mask '%s'. Check supported formatters", date, mask));
		}
	}
	
	public static String formatDate(Date date, String mask) {
		return new SimpleDateFormat(convertToJavaFormat(mask)).format(date);
	}
	
	public static SimpleDateFormat getFormatter(String mask) {
		return new SimpleDateFormatThreadSafe(convertToJavaFormat(mask));
	}

	private static String convertToJavaFormat(String mask) {
		mask = mask.toUpperCase();
		for(FormatMapper fmt : FormatMapper.values()) {
			mask = mask.replace(fmt.name(), fmt.getJavaFormat());
		}
		return mask.replace("_", "");
	}
}

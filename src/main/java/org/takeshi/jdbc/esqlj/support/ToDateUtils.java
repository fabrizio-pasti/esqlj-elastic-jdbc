package org.takeshi.jdbc.esqlj.support;

import java.sql.SQLSyntaxErrorException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

public class ToDateUtils {
	 
	private enum FormatMapper {
		YEAR("yyyy"),
		YYYY("yyyy"),
		YY("yy"),
		MM("MM"),
		MON("MM"),
		DD("dd"),
		HH("hh"),
		HH12("hh"),
		HH24("HH"),
		MI("mm"),
		SS("ss");
		
		private String javaFormat;
		
		FormatMapper(String javaFormat) {
			this.javaFormat = javaFormat;
		}
		
		public String getJavaFormat() {
			return javaFormat;
		}
	}
	
	public static Date resolveToDate(String date, String mask) throws SQLSyntaxErrorException {
		String format = convertToJavaFormat(mask); 
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		try {
			return sdf.parse(date);
		} catch (ParseException e) {
			throw new SQLSyntaxErrorException(String.format("Failed to parse date '%s' with mask '%s'. Check supported formatters", date, mask));
		}
	}

	private static String convertToJavaFormat(String mask) {
		StringBuilder sbMask = new StringBuilder(mask.toUpperCase());
		Arrays.stream(FormatMapper.values()).forEach(fmt -> {
			int iof = sbMask.indexOf(fmt.name());
			if(iof >= 0) {
				sbMask.replace(iof, iof + fmt.getJavaFormat().length(), fmt.getJavaFormat());
			}
		});
		return sbMask.toString();
	}
}

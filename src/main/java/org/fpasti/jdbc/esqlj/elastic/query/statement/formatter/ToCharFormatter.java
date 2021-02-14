package org.fpasti.jdbc.esqlj.elastic.query.statement.formatter;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

import org.fpasti.jdbc.esqlj.support.DateUtils;

import net.sf.jsqlparser.expression.Function;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class ToCharFormatter extends Formatter {

	private SimpleDateFormat sdf;
	
	public ToCharFormatter(Function function) {
		super(function);
	}

	@Override
	protected void init() {
		sdf = DateUtils.getFormatter(getStrValueParameter(1));
	}

	@Override
	public Object resolveValue(Object value) {
		return sdf.format(Date.from(((LocalDateTime)value).atZone(ZoneId.systemDefault()).toInstant()));
	}

}

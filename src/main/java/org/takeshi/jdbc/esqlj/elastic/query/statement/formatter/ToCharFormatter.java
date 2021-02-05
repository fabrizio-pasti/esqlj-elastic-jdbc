package org.takeshi.jdbc.esqlj.elastic.query.statement.formatter;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.takeshi.jdbc.esqlj.support.ToDateUtils;

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
		sdf = ToDateUtils.getFormatter(getStrValueParameter(1));
	}

	@Override
	public Object resolveValue(Object value) {
		return sdf.format(((Date)value));
	}

}

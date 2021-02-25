package org.fpasti.jdbc.esqlj.elastic.query.statement.formatter;

import java.sql.SQLDataException;

import org.fpasti.jdbc.esqlj.elastic.model.EsGeoPoint;
import org.fpasti.jdbc.esqlj.support.EsWrapException;

import net.sf.jsqlparser.expression.Function;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class LongitudeFormatter extends Formatter {
	
	public LongitudeFormatter(Function function) {
		super(function);
	}

	@Override
	protected void init() {
	}

	@Override
	public Object resolveValue(Object value) {
		if(value == null) {
			return null;
		}
		
		if(!(value instanceof EsGeoPoint)) {
			throw new EsWrapException(new SQLDataException("LATITUDE function expects a EsGeoPoint instance"));
		}
		
		return ((EsGeoPoint)value).getLongitude();
	}

}

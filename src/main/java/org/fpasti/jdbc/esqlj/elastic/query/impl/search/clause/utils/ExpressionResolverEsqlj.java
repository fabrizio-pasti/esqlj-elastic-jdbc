package org.fpasti.jdbc.esqlj.elastic.query.impl.search.clause.utils;

import java.sql.SQLSyntaxErrorException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.fpasti.jdbc.esqlj.elastic.query.impl.search.model.EvaluateQueryResult;
import org.fpasti.jdbc.esqlj.support.EsqljConstants;
import org.fpasti.jdbc.esqlj.support.Utils;

import net.sf.jsqlparser.statement.create.table.ColDataType;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class ExpressionResolverEsqlj {

	public static EvaluateQueryResult manageExpression(ColDataType colDataType) throws SQLSyntaxErrorException {
		String queryType = colDataType.getDataType().toLowerCase();
		List<String> arguments = colDataType.getArgumentsStringList().stream().map(arg -> arg.replace("'", "")).collect(Collectors.toList());
		
		switch(queryType) {
			case "query_string":
				return queryString(queryType, arguments);
			default:
				throw new SQLSyntaxErrorException(String.format("Unsupported '%s' query '%s'", EsqljConstants.ESQLJ_WHERE_CLAUSE, colDataType.getDataType()));
		}
	}

	private static EvaluateQueryResult queryString(String queryType, List<String> arguments) throws SQLSyntaxErrorException {
		checkMinimumNumberOfParameters(queryType, arguments, 2);
		String query = arguments.get(0);
		String[] fields = arguments.get(1).split(",");
		
		QueryStringQueryBuilder qsqb = QueryBuilders.queryStringQuery(query);
		Arrays.asList(fields).forEach(field -> {
			qsqb.field(field);
			});
		
		arguments.stream().skip(2).forEach(param -> {
			Utils.setAttributeInElasticObject(qsqb, getParameterName(param), getParameterValue(param));
		});
	
		return new EvaluateQueryResult(qsqb);
	}
	
	private static String getParameterName(String param) {
		return param.split(":")[0];
	}

	private static String getParameterValue(String param) {
		return param.split(":")[1];
	}

	private static void checkMinimumNumberOfParameters(String queryType, List<String> arguments, int numMinNumOfArguments) throws SQLSyntaxErrorException {
		if(arguments.size() < numMinNumOfArguments) {
			throw new SQLSyntaxErrorException(String.format("%s ::%s required at least %d parameters", EsqljConstants.ESQLJ_WHERE_CLAUSE, queryType, numMinNumOfArguments));
		}
	}
}

package org.takeshi.jdbc.esqlj.parser;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Stream.generate;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.takeshi.jdbc.esqlj.parser.model.Field;
import org.takeshi.jdbc.esqlj.parser.model.ParsedQuery;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.parser.Node;
import net.sf.jsqlparser.parser.SimpleNode;

public final class SqlParser {

	private SqlParser() {
	}

	public static ParsedQuery parse(String sql) throws JSQLParserException {
		Map<String, String> table = new LinkedHashMap<String, String>();
		ParsedQuery qry = new ParsedQuery();
		SimpleNode node = (SimpleNode) CCJSqlParserUtil.parseAST(sql);

		visit(node, 0);

		//visitTable(node, 0, table, qry);

		visitField(node, 0, table,qry);

		return qry;
	}

	private static void visit(SimpleNode node, int depth) {
		System.out.println(generate(() -> " ").limit(depth * 5).collect(joining()) + node.toString() + " : "
				+ (node.jjtGetValue() != null ? node.jjtGetValue().getClass().getSimpleName() : "") + "  ["
				+ node.jjtGetValue() + "]");

		for (int i = 0; i < node.jjtGetNumChildren(); i++) {
			visit((SimpleNode) node.jjtGetChild(i), depth + 1);
		}
	}

	private static void visitField(SimpleNode node, int depth, Map<String, String> table, ParsedQuery qry) throws JSQLParserException {

		switch (node.toString()) {
			case "SelectItem":
				qry.getFields().add(parseField(node.jjtGetValue().toString()));				
				break;
		}
	
		for (int i = 0; i < node.jjtGetNumChildren(); i++) {
			visitField((SimpleNode) node.jjtGetChild(i), depth + 1, table, qry);
		}
	}

	private static void visitTable(SimpleNode node, int depth, Map<String, String> table, ParsedQuery qry) {

		switch (node.toString()) {
		case "Table":
			
			String [] tableArray = node.jjtGetValue().toString().split(" ");
			
			if(tableArray.length > 1) {
				table.put(tableArray[1],  tableArray[0]);				
				qry.getIndex().setName(tableArray[0]);
				qry.getIndex().setAlias(tableArray[1]);				
			}else {
				qry.getIndex().setName(tableArray[0]);				
			}
			
			break;
		};

		for (int i = 0; i < node.jjtGetNumChildren(); i++) {
			visitTable((SimpleNode) node.jjtGetChild(i), depth + 1, table, qry);
		}

	}
	
	public static Field parseField(String value) throws JSQLParserException {
				
		String [] fieldArray = StringUtils.split(value," ");
               
        switch (fieldArray.length) {
			case 1:
				if(value.contains(".\"")) {
					String colName = StringUtils.split(fieldArray[0],".")[1] + "." +StringUtils.split(fieldArray[0],".")[2];
					return new Field(colName, null, StringUtils.split(fieldArray[0],".\"")[0]);
				}else if (value.contains("\"")){	
					return new Field(fieldArray[0], null, null);
				}else if (value.contains(".")){	
					return new Field(StringUtils.split(fieldArray[0],".")[1], null, StringUtils.split(fieldArray[0],".")[0]);
				}else {					
					return new Field(fieldArray[0], null, null);
				}	
			case 2:	
				if(value.contains(".\"")) {
					String colName = StringUtils.split(fieldArray[0],".")[1] + "." + StringUtils.split(fieldArray[0],".")[2];
					return new Field(colName, fieldArray[1], StringUtils.split(fieldArray[0],".\"")[0]);
				}else if (value.contains("\"")){	
					return new Field(fieldArray[0], fieldArray[1], null);
				}else if (value.contains(".")){	
					return new Field(StringUtils.split(fieldArray[0],".")[1], fieldArray[1], StringUtils.split(fieldArray[0],".")[0]);
				}else {					
					return new Field(fieldArray[0], fieldArray[1], null);
				}			
			case 3:	
				if(value.contains(".\"")) {
					String colName = StringUtils.split(fieldArray[0],".")[1] + "." + StringUtils.split(fieldArray[0],".")[2];
					return new Field(colName, fieldArray[2], StringUtils.split(fieldArray[0],".\"")[0]);
				}else if (value.contains("\"")){	
					return new Field(fieldArray[0], fieldArray[2], null);
				}else if (value.contains(".")){	
					return new Field(StringUtils.split(fieldArray[0],".")[1], fieldArray[2], StringUtils.split(fieldArray[0],".")[0]);
				}else {					
					return new Field(fieldArray[0], fieldArray[2], null);
				}
			default:
				throw new JSQLParserException("Bad select find in " + value ) ;			
		}
				
	}
	
}

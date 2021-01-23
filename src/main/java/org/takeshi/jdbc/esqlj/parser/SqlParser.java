package org.takeshi.jdbc.esqlj.parser;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Stream.generate;

import java.util.LinkedHashMap;
import java.util.Map;

import org.takeshi.jdbc.esqlj.parser.model.Field;
import org.takeshi.jdbc.esqlj.parser.model.ParsedQuery;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.parser.Node;
import net.sf.jsqlparser.parser.SimpleNode;

public final class SqlParser {

	private SqlParser() {
	}

	public static Node parse(String sql) throws JSQLParserException {
		Map<String, String> table = new LinkedHashMap<String, String>();
		ParsedQuery qry = new ParsedQuery();
		SimpleNode node = (SimpleNode) CCJSqlParserUtil.parseAST(sql);

		visit(node, 0);

		visitTable(node, 0, table, qry);

		visitField(node, 0, table,qry);

		return null;
	}

	private static void visit(SimpleNode node, int depth) {
		System.out.println(generate(() -> " ").limit(depth * 5).collect(joining()) + node.toString() + " : "
				+ (node.jjtGetValue() != null ? node.jjtGetValue().getClass().getSimpleName() : "") + "  ["
				+ node.jjtGetValue() + "]");

		for (int i = 0; i < node.jjtGetNumChildren(); i++) {
			visit((SimpleNode) node.jjtGetChild(i), depth + 1);
		}
	}

	private static void visitField(SimpleNode node, int depth, Map<String, String> table, ParsedQuery qry) {

		switch (node.toString()) {
			case "SelectItem":
				qry.getFields().add(new Field(node.jjtGetLastToken().toString().equals("*") ? "*" : node.jjtGetLastToken().toString(), 
						node.jjtGetLastToken().toString().equals("*") ? null : null, null));				
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
	
	public static Field parseField(String value) {
		
		System.out.println(value);
		
		if (value.contains(".") & value.toLowerCase().contains("as")) {
			//String index = value.contains(".");
			System.out.println(value);
		} else {
			System.out.println(value);
		}
		
		return null;
		
		
	}
	
}

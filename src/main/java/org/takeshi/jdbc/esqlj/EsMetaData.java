package org.takeshi.jdbc.esqlj;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.takeshi.jdbc.esqlj.elastic.metadata.MetaDataService;
import org.takeshi.jdbc.esqlj.elastic.model.ElasticField;
import org.takeshi.jdbc.esqlj.elastic.model.ElasticFieldType;
import org.takeshi.jdbc.esqlj.elastic.model.ElasticObjectType;
import org.takeshi.jdbc.esqlj.elastic.query.impl.FromArrayQuery;
import org.takeshi.jdbc.esqlj.elastic.query.impl.IndexFieldsQuery;
import org.takeshi.jdbc.esqlj.elastic.query.impl.IndicesQuery;
import org.takeshi.jdbc.esqlj.support.EsConfig;
import org.takeshi.jdbc.esqlj.support.EsConfig.ConfigurationPropertyEnum;

public class EsMetaData implements DatabaseMetaData {
	private EsConnection connection;
	private MetaDataService metaDataService;

	private static String DRIVER_NAME = "esqlj";
	private static String DRIVER_VERSION = "1.0.0";
	private static int DRIVER_MAJOR_VERSION = 7;
	private static int DRIVER_MINOR_VERSION = 6;

	protected EsMetaData(EsConnection connection) throws SQLException {
		this.connection = connection;
		this.metaDataService = new MetaDataService(connection.getElasticClient());
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return iface.cast(this);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return iface.isInstance(this);
	}

	@Override
	public boolean allProceduresAreCallable() throws SQLException {
		return false;
	}

	@Override
	public boolean allTablesAreSelectable() throws SQLException {
		return true;
	}

	@Override
	public String getURL() throws SQLException {
		return EsConfig.getUrl();
	}

	@Override
	public String getUserName() throws SQLException {
		return EsConfig.getConfiguration(ConfigurationPropertyEnum.USERNAME, String.class);
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return false;
	}

	@Override
	public boolean nullsAreSortedHigh() throws SQLException { // tocheck
		return false;
	}

	@Override
	public boolean nullsAreSortedLow() throws SQLException { // tocheck
		return false;
	}

	@Override
	public boolean nullsAreSortedAtStart() throws SQLException { // tocheck
		return false;
	}

	@Override
	public boolean nullsAreSortedAtEnd() throws SQLException { // tocheck
		return false;
	}

	@Override
	public String getDatabaseProductName() throws SQLException {
		return metaDataService.getProductName();
	}

	@Override
	public String getDatabaseProductVersion() throws SQLException {
		return metaDataService.getElasticServerDetails().getNumber();
	}

	@Override
	public String getDriverName() throws SQLException {
		return DRIVER_NAME;
	}

	@Override
	public String getDriverVersion() throws SQLException {
		return DRIVER_VERSION;
	}

	@Override
	public int getDriverMajorVersion() {
		return DRIVER_MAJOR_VERSION;
	}

	@Override
	public int getDriverMinorVersion() {
		return DRIVER_MINOR_VERSION;
	}

	@Override
	public boolean usesLocalFiles() throws SQLException {
		return false;
	}

	@Override
	public boolean usesLocalFilePerTable() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMixedCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesUpperCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesLowerCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesMixedCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
		return true;
	}

	@Override
	public String getIdentifierQuoteString() throws SQLException {
		return "\"";
	}

	@Override
	public String getSQLKeywords() throws SQLException { // add not sql 2003 keywords
		return "";
	}

	@Override
	public String getNumericFunctions() throws SQLException { // add numeric functions
		return "";
	}

	@Override
	public String getStringFunctions() throws SQLException { // add string functions
		return "";
	}

	@Override
	public String getSystemFunctions() throws SQLException { // add system functions
		return "";
	}

	@Override
	public String getTimeDateFunctions() throws SQLException { // add timedate functions
		return "";
	}

	@Override
	public String getSearchStringEscape() throws SQLException {
		return "";
	}

	@Override
	public String getExtraNameCharacters() throws SQLException { // todo: to check
		return "_";
	}

	@Override
	public boolean supportsAlterTableWithAddColumn() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsAlterTableWithDropColumn() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsColumnAliasing() throws SQLException {
		return false;
	}

	@Override
	public boolean nullPlusNonNullIsNull() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsConvert() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsConvert(int fromType, int toType) throws SQLException {
		return false;
	}

	@Override
	public boolean supportsTableCorrelationNames() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsDifferentTableCorrelationNames() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsExpressionsInOrderBy() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOrderByUnrelated() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsGroupBy() throws SQLException { // need to be implemented
		return true;
	}

	@Override
	public boolean supportsGroupByUnrelated() throws SQLException { // need to be implemented
		return true;
	}

	@Override
	public boolean supportsGroupByBeyondSelect() throws SQLException { // need to be implemented
		return true;
	}

	@Override
	public boolean supportsLikeEscapeClause() throws SQLException { // need to be implemented
		return true;
	}

	@Override
	public boolean supportsMultipleResultSets() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsMultipleTransactions() throws SQLException { // need to be implemented
		return true;
	}

	@Override
	public boolean supportsNonNullableColumns() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsMinimumSQLGrammar() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsCoreSQLGrammar() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsExtendedSQLGrammar() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsANSI92EntryLevelSQL() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsANSI92IntermediateSQL() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsANSI92FullSQL() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsIntegrityEnhancementFacility() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOuterJoins() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsFullOuterJoins() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsLimitedOuterJoins() throws SQLException {
		return false;
	}

	@Override
	public String getSchemaTerm() throws SQLException {
		return "";
	}

	@Override
	public String getProcedureTerm() throws SQLException {
		return "";
	}

	@Override
	public String getCatalogTerm() throws SQLException {
		return "";
	}

	@Override
	public boolean isCatalogAtStart() throws SQLException {
		return true;
	}

	@Override
	public String getCatalogSeparator() throws SQLException {
		return ".";
	}

	@Override
	public boolean supportsSchemasInDataManipulation() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInProcedureCalls() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInTableDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInIndexDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInDataManipulation() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInProcedureCalls() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInTableDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsPositionedDelete() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsPositionedUpdate() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSelectForUpdate() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsStoredProcedures() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSubqueriesInComparisons() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSubqueriesInExists() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSubqueriesInIns() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSubqueriesInQuantifieds() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCorrelatedSubqueries() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsUnion() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsUnionAll() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
		return false;
	}

	@Override
	public int getMaxBinaryLiteralLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxCharLiteralLength() throws SQLException {
		return 255;
	}

	@Override
	public int getMaxColumnNameLength() throws SQLException {
		return 255;
	}

	@Override
	public int getMaxColumnsInGroupBy() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInIndex() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInOrderBy() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInSelect() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInTable() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxConnections() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxCursorNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxIndexLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxSchemaNameLength() throws SQLException {
		return 255;
	}

	@Override
	public int getMaxProcedureNameLength() throws SQLException {
		return 255;
	}

	@Override
	public int getMaxCatalogNameLength() throws SQLException {
		return 255;
	}

	@Override
	public int getMaxRowSize() throws SQLException {
		return 0;
	}

	@Override
	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
		return false;
	}

	@Override
	public int getMaxStatementLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxStatements() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxTableNameLength() throws SQLException {
		return 255;
	}

	@Override
	public int getMaxTablesInSelect() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxUserNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getDefaultTransactionIsolation() throws SQLException {
		return Connection.TRANSACTION_NONE;
	}

	@Override
	public boolean supportsTransactions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
		return false;
	}

	@Override
	public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
		return false;
	}

	@Override
	public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
		return false;
	}

	@Override
	public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
		return false;
	}

	@Override
	public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
			throws SQLException {
		return null;
	}

	@Override
	public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
			String columnNamePattern) throws SQLException {
		return null;
	}

	@Override
	public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
			throws SQLException {
		return new EsResultSet(new IndicesQuery(connection, ElasticObjectType.INDEX, ElasticObjectType.ALIAS));
	}

	@Override
	public ResultSet getSchemas() throws SQLException {
		return new EsResultSet(
				new FromArrayQuery("system_schemas", Arrays.asList(Arrays.asList(metaDataService.getElasticServerDetails().getClusterName())), "TABLE_SCHEM"));
	}

	@Override
	public ResultSet getCatalogs() throws SQLException {
		return new EsResultSet(
				new FromArrayQuery("system_catalogs", Arrays.asList(Arrays.asList("")), "TABLE_CAT"));
	}

	@Override
	public ResultSet getTableTypes() throws SQLException {
		return new EsResultSet(new FromArrayQuery("system_catalogs",
				Arrays.asList(Arrays.asList("TABLE"),Arrays.asList("VIEW")), "TABLE_TYPE"));
	}

	@Override
	public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
			throws SQLException { 
		return new EsResultSet(new IndexFieldsQuery(connection, tableNamePattern, columnNamePattern));
	}

	@Override
	public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
			throws SQLException {
		return null;
	}

	@Override
	public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
			throws SQLException {
		// TODO
		return null;
	}

	@Override
	public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
			throws SQLException {
		return new EsResultSet(new FromArrayQuery(table,
				Arrays.asList(
						Arrays.asList(2, "_id", Types.VARCHAR, "VARCHAR", 0, 0, 0, 0)), "SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "PSEUDO_COLUMN"));
	}

	@Override
	public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
		return null;
	}
		
	@Override
	public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
		return new EsResultSet(new FromArrayQuery(table,
				Arrays.asList(
						Arrays.asList(catalog, schema, table, "_id", 1, "_id")), "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"));
	}

	@Override
	public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
		// TODO 
		return null;
	}

	@Override
	public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
		// TODO 
		return null;
	}

	@Override
	public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
			String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
		// TODO
		return null;
	}

	@Override
	public ResultSet getTypeInfo() throws SQLException {
		return new EsResultSet(new FromArrayQuery("system_catalogs",
				Arrays.asList(ElasticFieldType.values()).stream().filter(t -> t.isConcrete()).map(t -> new ArrayList<Object>(Arrays.asList(t.getSqlType(), t.getSqlTypeCode(), t.isCaseSensitive(), t.getLiteralPrefix(), t.getLiteralSuffix(), null, t.isCaseSensitive(), 1, t.isUnsigned(), false, false, t.getSqlType(), null, null, null, null, 10))).collect(Collectors.toList()), 
				"TYPE_NAME", "DATA_TYPE", "PRECISION", "LITERAL_SUFFIX", "CREATE_PARAMS", "NULLABLE", "CASE_SENSITIVE", "SEARCHABLE", "UNSIGNED_ATTRIBUTE", "FIXED_PREC_SCALE", "AUTO_INCREMENT", "LOCAL_TYPE_NAME", "MINIMUM_SCALE", "MAXIMUM_SCALE", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "NUM_PREC_RADIX"));
	}

	@Override
	public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
			throws SQLException {
		return null;
	}

	@Override
	public boolean supportsResultSetType(int type) throws SQLException {
		return type == ResultSet.TYPE_FORWARD_ONLY;
	}

	@Override
	public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
		return concurrency == ResultSet.CONCUR_READ_ONLY;
	}

	@Override
	public boolean ownUpdatesAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean ownDeletesAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean ownInsertsAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean othersUpdatesAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean othersDeletesAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean othersInsertsAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean updatesAreDetected(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean deletesAreDetected(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean insertsAreDetected(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean supportsBatchUpdates() throws SQLException {
		return false;
	}

	@Override
	public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
			throws SQLException {
		return new EsResultSet(new FromArrayQuery("system_catalogs",
				Arrays.stream(ElasticFieldType.values()).filter(t -> t.isUdt()).map(t -> new ArrayList<Object>(Arrays.asList(catalog, schemaPattern, t.getSqlType(), t.getClass().getName(), t.getSqlTypeCode(), null, null))).collect(Collectors.toList()), 
				"TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "CLASS_NAME", "DATA_TYPE", "REMARKS", "BASE_TYPE"));
	}

	@Override
	public Connection getConnection() throws SQLException {
		return connection;
	}

	@Override
	public boolean supportsSavepoints() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsNamedParameters() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMultipleOpenResults() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsGetGeneratedKeys() throws SQLException {
		return true;
	}

	@Override
	public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
		return new EsResultSet(new FromArrayQuery("system_catalogs",
				new ArrayList<List<Object>>(), "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SUPERTYPE_CAT", "SUPERTYPE_SCHEM", "", "SUPERTYPE_NAME"));
	}

	@Override
	public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
		return new EsResultSet(new FromArrayQuery("system_catalogs",
				new ArrayList<List<Object>>(), "TYPE_CAT", "TYPE_SCHEM", "TABLE_NAME", "SUPERTABLE_NAME"));
	}

	@Override
	public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
			String attributeNamePattern) throws SQLException {
		return new EsResultSet(new FromArrayQuery("system_catalogs",
				new ArrayList<List<Object>>(), "TYPE_CAT", "TYPE_SCHEM", "TABLE_NAME", "ATTR_NAME", "DATA_TYPE", "ATTR_TYPE_NAME", "ATTR_SIZE", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "ATTR_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE"));
	}

	@Override
	public boolean supportsResultSetHoldability(int holdability) throws SQLException {
		return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		return ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	@Override
	public int getDatabaseMajorVersion() throws SQLException {
		return DRIVER_MAJOR_VERSION;
	}

	@Override
	public int getDatabaseMinorVersion() throws SQLException {
		return DRIVER_MINOR_VERSION;
	}

	@Override
	public int getJDBCMajorVersion() throws SQLException {
		return DRIVER_MAJOR_VERSION;
	}

	@Override
	public int getJDBCMinorVersion() throws SQLException {
		return DRIVER_MINOR_VERSION;
	}

	@Override
	public int getSQLStateType() throws SQLException {
		return 0;
	}

	@Override
	public boolean locatorsUpdateCopy() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsStatementPooling() throws SQLException {
		return true;
	}

	@Override
	public RowIdLifetime getRowIdLifetime() throws SQLException {
		return RowIdLifetime.ROWID_VALID_FOREVER;
	}

	@Override
	public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
		return new EsResultSet(new FromArrayQuery("system_catalogs",
				new ArrayList<List<Object>>(), "TABLE_SCHEM ", "TABLE_CATALOG"));
	}

	@Override
	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
		return false;
	}

	@Override
	public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
		return false;
	}

	@Override
	public ResultSet getClientInfoProperties() throws SQLException {
		return new EsResultSet(new FromArrayQuery("system_catalogs",
				new ArrayList<List<Object>>(), "NAME ", "MAX_LEN", "DEFAULT_VALUE", "DESCRIPTION"));
	}

	@Override
	public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
			throws SQLException {
		return new EsResultSet(new FromArrayQuery("system_catalogs",
				new ArrayList<List<Object>>(), "FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "REMARKS", "FUNCTION_TYPE", "SPECIFIC_NAME"));
	}
			
	@Override
	public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
			String columnNamePattern) throws SQLException {
		return new EsResultSet(new FromArrayQuery("system_catalogs",
				new ArrayList<List<Object>>(), "FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "COLUMN_NAME", "FUNCTION_TYPE", "SPECIFIC_NAME", "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME", "PRECISION", "LENGTH", "SCALE", "RADIX", "NULLABLE", "REMARKS", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME"));
	}
	
	@Override
	public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
			String columnNamePattern) throws SQLException {
		return new EsResultSet(new FromArrayQuery("system_catalogs",
				new ArrayList<List<Object>>(), "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "COLUMN_SIZE", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "COLUMN_USAGE", "REMARKS", "CHAR_OCTET_LENGTH", "IS_NULLABLE"));
	}

	@Override
	public boolean generatedKeyAlwaysReturned() throws SQLException {
		return false;
	}
	
	public MetaDataService getMetaDataService() {
		return metaDataService;
	}

}

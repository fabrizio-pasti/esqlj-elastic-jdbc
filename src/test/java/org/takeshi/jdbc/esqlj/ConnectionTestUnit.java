package org.takeshi.jdbc.esqlj;

import static junitparams.JUnitParamsRunner.$;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.takeshi.jdbc.esqlj.support.EsConfig;
import org.takeshi.jdbc.esqlj.support.ElasticInstance.HttpProtocol;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

@RunWith(JUnitParamsRunner.class)
public class ConnectionTestUnit {

	@SuppressWarnings({ "unused", "deprecation" })
	private static final Object[] urls() {
		return $($(true, "jdbc:esqlj:http://153.77.137.170:9200"), $(true, "jdbc:esqlj:http://elastic.server.com:9200"),
				$(true, "jdbc:esqlj:https://elastic.server.com"), $(true, "jdbc:esqlj:https://153.77.137.170:9200"),
				$(true, "jdbc:esqlj:http://153.77.137.170:9200,http://153.77.137.171:9200"),
				$(true, "jdbc:esqlj:http://153.77.137.170:9200,http://elastic.server.com:9200"),
				$(true, "jdbc:esqlj:http://153.77.137.170"), $(true, "jdbc:esqlj:http://elastic.server.com"),
				$(false, "jdb:esqlj:http://153.77.137.170:9200"), $(false, "jdb:esql:http://153.77.137.170:9200"),
				$(false, "jdbc:esqlj:htt://:9200"), $(false, "jdbc:esqlj:http:/153.77.137.170:9200"),
				$(false, "jdbc:esqlj:http//153.77.137.170:9200"), $(false, "jdbc:esqlj://153.77.137.170:9200"),
				$(false, "jdbc:esqlj://153.77.137.170"), $(false, "jdbc:esqlj://153.77.137.170:9200"),
				$(false, "jdbc:esqlj:http://153.77.137.170:9200,//elastic.server.com:9200"));
	}
	
	@SuppressWarnings({ "unused", "deprecation" })
	private static final Object[] urls2() {
		return $(
				$("jdbc:esqlj:http://153.77.137.170:9200", HttpProtocol.http, "153.77.137.170", 9200),
				$("jdbc:esqlj:http://153.77.137.170", HttpProtocol.http, "153.77.137.170", 9200),
				$("jdbc:esqlj:https://elastic.server.com:9201", HttpProtocol.https, "elastic.server.com", 9201)
			);
	}

	@Test
	@Parameters(method = "urls")
	public void checkUrl(boolean valid, String url) throws SQLException {
		if (valid) {
			Connection connection = TestUtils.getTestConnection(url, null);
			assertNotNull(connection);
		} else {
			SQLException e = assertThrows(SQLException.class, () -> {
				TestUtils.getTestConnection(url, null);
			});
			assertEquals(e.getMessage(), "Invalid connection string");
		}
	}

	@Test
	@Parameters(method = "urls2")
	public void checkUrl2(String url, HttpProtocol protocol, String server, int port) throws SQLException {
		TestUtils.getTestConnection(url, null);
		assertEquals(EsConfig.getUrls().get(0).getProtocol(), protocol);
		assertEquals(EsConfig.getUrls().get(0).getServer(), server);
		assertEquals(EsConfig.getUrls().get(0).getPort(), port);
	}
	
	/*
	 * @Test public void isValid() throws SQLException { Connection conn =
	 * DriverManager.getConnection("jdbc:sqlite:"); assertTrue(conn.isValid(0));
	 * conn.close(); assertFalse(conn.isValid(0)); }
	 * 
	 * @Test public void executeUpdateOnClosedDB() throws SQLException { Connection
	 * conn = DriverManager.getConnection("jdbc:sqlite:"); Statement stat =
	 * conn.createStatement(); conn.close();
	 * 
	 * try { stat.executeUpdate("create table A(id, name)"); } catch (SQLException
	 * e) { return; // successfully detect the operation on the closed DB }
	 * fail("should not reach here"); }
	 * 
	 * @Test public void readOnly() throws SQLException {
	 * 
	 * // set read only mode SQLiteConfig config = new SQLiteConfig();
	 * config.setReadOnly(true);
	 * 
	 * Connection conn = DriverManager.getConnection("jdbc:sqlite:",
	 * config.toProperties()); Statement stat = conn.createStatement(); try {
	 * assertTrue(conn.isReadOnly()); // these updates must be forbidden in
	 * read-only mode stat.executeUpdate("create table A(id, name)");
	 * stat.executeUpdate("insert into A values(1, 'leo')");
	 * 
	 * fail("read only flag is not properly set"); } catch (SQLException e) { //
	 * success } finally { stat.close(); conn.close(); }
	 * 
	 * config.setReadOnly(true); // should be a no-op
	 * 
	 * try{ conn.setReadOnly(false);
	 * fail("should not change read only flag after opening connection"); } catch
	 * (SQLException e) { assert(e.getMessage().
	 * contains("Cannot change read-only flag after establishing a connection.")); }
	 * finally { conn.close(); } }
	 * 
	 * @Test public void foreignKeys() throws SQLException { SQLiteConfig config =
	 * new SQLiteConfig(); config.enforceForeignKeys(true); Connection conn =
	 * DriverManager.getConnection("jdbc:sqlite:", config.toProperties()); Statement
	 * stat = conn.createStatement();
	 * 
	 * try { stat.
	 * executeUpdate("create table track(id integer primary key, name, aid, foreign key (aid) references artist(id))"
	 * ); stat.executeUpdate("create table artist(id integer primary key, name)");
	 * 
	 * stat.executeUpdate("insert into artist values(10, 'leo')");
	 * stat.executeUpdate("insert into track values(1, 'first track', 10)"); // OK
	 * 
	 * try { stat.executeUpdate("insert into track values(2, 'second track', 3)");
	 * // invalid reference } catch (SQLException e) { return; // successfully
	 * detect violation of foreign key constraints }
	 * fail("foreign key constraint must be enforced"); } finally { stat.close();
	 * conn.close(); }
	 * 
	 * }
	 * 
	 * @Test public void canWrite() throws SQLException { SQLiteConfig config = new
	 * SQLiteConfig(); config.enforceForeignKeys(true); Connection conn =
	 * DriverManager.getConnection("jdbc:sqlite:", config.toProperties()); Statement
	 * stat = conn.createStatement();
	 * 
	 * try { assertFalse(conn.isReadOnly()); } finally { stat.close(); conn.close();
	 * }
	 * 
	 * }
	 * 
	 * @Test public void synchronous() throws SQLException { SQLiteConfig config =
	 * new SQLiteConfig(); config.setSynchronous(SynchronousMode.OFF); Connection
	 * conn = DriverManager.getConnection("jdbc:sqlite:", config.toProperties());
	 * Statement stat = conn.createStatement();
	 * 
	 * try { ResultSet rs = stat.executeQuery("pragma synchronous"); if (rs.next())
	 * { ResultSetMetaData rm = rs.getMetaData(); int i = rm.getColumnCount(); int
	 * synchronous = rs.getInt(1); assertEquals(0, synchronous); }
	 * 
	 * } finally { stat.close(); conn.close(); }
	 * 
	 * }
	 * 
	 * @Test public void openMemory() throws SQLException { Connection conn =
	 * DriverManager.getConnection("jdbc:sqlite:"); conn.close(); }
	 * 
	 * @Test public void isClosed() throws SQLException { Connection conn =
	 * DriverManager.getConnection("jdbc:sqlite:"); conn.close();
	 * assertTrue(conn.isClosed()); }
	 * 
	 * @Test(expected = SQLException.class) public void closeTest() throws
	 * SQLException { Connection conn = DriverManager.getConnection("jdbc:sqlite:");
	 * PreparedStatement prep = conn.prepareStatement("select null;"); ResultSet rs
	 * = prep.executeQuery(); conn.close(); prep.clearParameters(); }
	 * 
	 * @Test(expected = SQLException.class) public void openInvalidLocation() throws
	 * SQLException { Connection conn =
	 * DriverManager.getConnection("jdbc:sqlite:/"); conn.close(); }
	 * 
	 * @Test public void openResource() throws Exception { File testDB =
	 * copyToTemp("sample.db"); assertTrue(testDB.exists()); Connection conn =
	 * DriverManager .getConnection(String.format("jdbc:sqlite::resource:%s",
	 * testDB.toURI().toURL())); Statement stat = conn.createStatement(); ResultSet
	 * rs = stat.executeQuery("select * from coordinate"); assertTrue(rs.next());
	 * rs.close(); stat.close(); conn.close();
	 * 
	 * }
	 * 
	 * @Test public void openJARResource() throws Exception { File testJAR =
	 * copyToTemp("testdb.jar"); assertTrue(testJAR.exists());
	 * 
	 * Connection conn = DriverManager.getConnection(String.format(
	 * "jdbc:sqlite::resource:jar:%s!/sample.db", testJAR .toURI().toURL()));
	 * Statement stat = conn.createStatement(); ResultSet rs =
	 * stat.executeQuery("select * from coordinate"); assertTrue(rs.next());
	 * rs.close(); stat.close(); conn.close(); }
	 * 
	 * @Test public void openFile() throws Exception {
	 * 
	 * File testDB = copyToTemp("sample.db");
	 * 
	 * assertTrue(testDB.exists()); Connection conn =
	 * DriverManager.getConnection(String.format("jdbc:sqlite:%s", testDB));
	 * conn.close(); }
	 * 
	 * @Test public void concurrentClose() throws SQLException,
	 * InterruptedException, ExecutionException { final Connection conn =
	 * DriverManager.getConnection("jdbc:sqlite:"); ResultSet[] rss = new
	 * ResultSet[512]; for (int i = 0; i < rss.length; i++) rss[i] =
	 * conn.prepareStatement("select null;").executeQuery(); ExecutorService
	 * finalizer = Executors.newSingleThreadExecutor(); try {
	 * ArrayList<Future<Void>> futures = new ArrayList<Future<Void>>(rss.length);
	 * for (final ResultSet rs: rss) futures.add(finalizer.submit(new
	 * Callable<Void>() { public Void call() throws Exception { rs.close(); return
	 * null; } })); conn.close(); for (Future<Void> f: futures) f.get(); } finally {
	 * finalizer.shutdown(); } }
	 * 
	 * public static File copyToTemp(String fileName) throws IOException {
	 * InputStream in = ConnectionTestUnit.class.getResourceAsStream(fileName); File
	 * dir = new File("target"); if (!dir.exists()) dir.mkdirs();
	 * 
	 * File tmp = File.createTempFile(fileName, "", new File("target"));
	 * tmp.deleteOnExit(); FileOutputStream out = new FileOutputStream(tmp);
	 * 
	 * byte[] buf = new byte[8192]; for (int readBytes = 0; (readBytes =
	 * in.read(buf)) != -1;) { out.write(buf, 0, readBytes); } out.flush();
	 * out.close(); in.close();
	 * 
	 * return tmp; }
	 * 
	 * @Test public void URIFilenames() throws SQLException { Connection conn1 =
	 * DriverManager.getConnection(
	 * "jdbc:sqlite:file:memdb1?mode=memory&cache=shared"); Statement stmt1 =
	 * conn1.createStatement(); stmt1.executeUpdate("create table tbl (col int)");
	 * stmt1.executeUpdate("insert into tbl values(100)"); stmt1.close();
	 * 
	 * Connection conn2 = DriverManager.getConnection(
	 * "jdbc:sqlite:file:memdb1?mode=memory&cache=shared"); Statement stmt2 =
	 * conn2.createStatement(); ResultSet rs =
	 * stmt2.executeQuery("select * from tbl"); assertTrue(rs.next());
	 * assertEquals(100, rs.getInt(1)); stmt2.close();
	 * 
	 * Connection conn3 =
	 * DriverManager.getConnection("jdbc:sqlite:file::memory:?cache=shared");
	 * Statement stmt3 = conn3.createStatement();
	 * stmt3.executeUpdate("attach 'file:memdb1?mode=memory&cache=shared' as memdb1"
	 * ); rs = stmt3.executeQuery("select * from memdb1.tbl");
	 * assertTrue(rs.next()); assertEquals(100, rs.getInt(1));
	 * stmt3.executeUpdate("create table tbl2(col int)");
	 * stmt3.executeUpdate("insert into tbl2 values(200)"); stmt3.close();
	 * 
	 * Connection conn4 =
	 * DriverManager.getConnection("jdbc:sqlite:file::memory:?cache=shared");
	 * Statement stmt4 = conn4.createStatement(); rs =
	 * stmt4.executeQuery("select * from tbl2"); assertTrue(rs.next());
	 * assertEquals(200, rs.getInt(1)); rs.close(); stmt4.close(); conn4.close(); }
	 * 
	 * @Test public void setPragmasFromURI() throws Exception { File testDB =
	 * copyToTemp("sample.db");
	 * 
	 * assertTrue(testDB.exists()); Connection conn =
	 * DriverManager.getConnection(String.format(
	 * "jdbc:sqlite:%s?journal_mode=WAL&synchronous=OFF&journal_size_limit=500",
	 * testDB)); Statement stat = conn.createStatement();
	 * 
	 * ResultSet rs = stat.executeQuery("pragma journal_mode"); assertEquals("wal",
	 * rs.getString(1)); rs.close();
	 * 
	 * rs = stat.executeQuery("pragma synchronous"); assertEquals(false,
	 * rs.getBoolean(1)); rs.close();
	 * 
	 * rs = stat.executeQuery("pragma journal_size_limit"); assertEquals(500,
	 * rs.getInt(1)); rs.close();
	 * 
	 * stat.close(); conn.close(); }
	 * 
	 * @Test(expected = SQLException.class) public void limits() throws Exception{
	 * File testDB = copyToTemp("sample.db");
	 * 
	 * assertTrue(testDB.exists()); Connection conn =
	 * DriverManager.getConnection(String.format("jdbc:sqlite:%s?limit_attached=0",
	 * testDB)); Statement stat = conn.createStatement();
	 * 
	 * stat.executeUpdate("ATTACH DATABASE attach_test.db AS attachDb");
	 * 
	 * stat.close(); }
	 * 
	 * @Test public void ignoreUnknownParametersInURI() throws Exception {
	 * Connection conn = DriverManager.getConnection(
	 * "jdbc:sqlite:file::memory:?cache=shared&foreign_keys=ON&debug=&invalid");
	 * Statement stat = conn.createStatement();
	 * 
	 * ResultSet rs = stat.executeQuery("pragma foreign_keys"); assertEquals(true,
	 * rs.getBoolean(1)); rs.close();
	 * 
	 * stat.close(); conn.close(); }
	 * 
	 * @Test(expected = SQLException.class) public void
	 * errorOnEmptyPragmaValueInURI() throws Exception {
	 * DriverManager.getConnection(
	 * "jdbc:sqlite:file::memory:?journal_mode=&synchronous="); }
	 * 
	 * @Test public void ignoreDoubleAmpersandsInURI() throws Exception { File
	 * testDB = copyToTemp("sample.db");
	 * 
	 * assertTrue(testDB.exists()); Connection conn =
	 * DriverManager.getConnection(String.format(
	 * "jdbc:sqlite:%s?synchronous=OFF&&&&journal_mode=WAL", testDB)); Statement
	 * stat = conn.createStatement();
	 * 
	 * ResultSet rs = stat.executeQuery("pragma journal_mode"); assertEquals("wal",
	 * rs.getString(1)); rs.close();
	 * 
	 * rs = stat.executeQuery("pragma synchronous"); assertEquals(false,
	 * rs.getBoolean(1)); rs.close();
	 * 
	 * stat.close(); conn.close(); }
	 * 
	 * @Test public void useLastSpecifiedPragmaValueInURI() throws Exception { File
	 * testDB = copyToTemp("sample.db");
	 * 
	 * assertTrue(testDB.exists()); Connection conn =
	 * DriverManager.getConnection(String.format(
	 * "jdbc:sqlite:%s?journal_mode=WAL&journal_mode=MEMORY&journal_mode=TRUNCATE",
	 * testDB)); Statement stat = conn.createStatement();
	 * 
	 * ResultSet rs = stat.executeQuery("pragma journal_mode");
	 * assertEquals("truncate", rs.getString(1)); rs.close();
	 * 
	 * stat.close(); conn.close(); }
	 * 
	 * @Test public void overrideURIPragmaValuesWithProperties() throws Exception {
	 * File testDB = copyToTemp("sample.db");
	 * 
	 * assertTrue(testDB.exists()); Properties props = new Properties();
	 * props.setProperty(Pragma.JOURNAL_MODE.pragmaName,
	 * JournalMode.TRUNCATE.name()); Connection conn =
	 * DriverManager.getConnection(String.format("jdbc:sqlite:%s?journal_mode=WAL",
	 * testDB), props); Statement stat = conn.createStatement();
	 * 
	 * ResultSet rs = stat.executeQuery("pragma journal_mode");
	 * assertEquals("truncate", rs.getString(1)); rs.close();
	 * 
	 * stat.close(); conn.close(); }
	 */
}

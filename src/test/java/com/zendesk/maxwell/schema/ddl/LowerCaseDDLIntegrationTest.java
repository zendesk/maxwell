package com.zendesk.maxwell.schema.ddl;

import java.io.File;

import com.zendesk.maxwell.MysqlIsolatedServer;
import com.zendesk.maxwell.MaxwellTestSupport;

import org.junit.*;

public class LowerCaseDDLIntegrationTest {
	protected static MysqlIsolatedServer convertServer;
	protected static MysqlIsolatedServer caseSensitiveServer;


	@BeforeClass
	public static void checkVersion() throws Exception {
		org.junit.Assume.assumeTrue(MysqlIsolatedServer.getVersion().lessThan(8,0));
	}

	@BeforeClass
	public static void setupServers() throws Exception {
		convertServer = MaxwellTestSupport.setupServer("--lower-case-table-names=1");
		MaxwellTestSupport.setupSchema(convertServer);

		if ( isFileSystemCaseSensitive() ) {
			caseSensitiveServer = MaxwellTestSupport.setupServer("--lower-case-table-names=0");
			MaxwellTestSupport.setupSchema(caseSensitiveServer);
		}
	}


	static Boolean caseSensitive = null;
	public static boolean isFileSystemCaseSensitive() throws Exception {
		if ( caseSensitive != null )
			return caseSensitive;

		File testFile = File.createTempFile("maxwell-CASE-sensitive", "aa");
		testFile.setLastModified(System.currentTimeMillis());

		boolean exists = new File(testFile.getAbsolutePath().toLowerCase()).exists();
		testFile.delete();

		caseSensitive = !exists;
		return caseSensitive;
	}

	@Test
	public void TestLowerCasingTableCreate() throws Exception {
		String sql[] = {
			"create TABLE taybal( a long varchar character set 'utf8' )",
			"alter table TAYbal add column b int",
			"drop table TAYBAL"
		};

		MaxwellTestSupport.testDDLFollowing(convertServer, sql);
	}

	@Test
	public void TestLowerCasingTableRename() throws Exception {
		String sql[] = {
			"create TABLE ttt( a int )",
			"rename table ttt to TTTT",
			"create table ttRR like tttt"
		};

		MaxwellTestSupport.testDDLFollowing(convertServer, sql);
	}
}

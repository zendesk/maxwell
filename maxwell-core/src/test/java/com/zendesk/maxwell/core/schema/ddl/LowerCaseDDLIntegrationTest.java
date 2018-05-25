package com.zendesk.maxwell.core.schema.ddl;

import com.zendesk.maxwell.core.MysqlIsolatedServer;
import com.zendesk.maxwell.core.support.MysqlIsolatedServerTestSupport;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

public class LowerCaseDDLIntegrationTest {
	protected static MysqlIsolatedServer convertServer;
	protected static MysqlIsolatedServer caseSensitiveServer;

	@BeforeClass
	public static void setupServers() throws Exception {
		convertServer = MysqlIsolatedServerTestSupport.setupServer("--lower-case-table-names=1");
		MysqlIsolatedServerTestSupport.setupSchema(convertServer);

		if ( isFileSystemCaseSensitive() ) {
			caseSensitiveServer = MysqlIsolatedServerTestSupport.setupServer("--lower-case-table-names=0");
			MysqlIsolatedServerTestSupport.setupSchema(caseSensitiveServer);
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

		MysqlIsolatedServerTestSupport.testDDLFollowing(convertServer, sql);
	}

	@Test
	public void TestLowerCasingTableRename() throws Exception {
		String sql[] = {
			"create TABLE ttt( a int )",
			"rename table ttt to TTTT",
			"create table ttRR like tttt"
		};

		MysqlIsolatedServerTestSupport.testDDLFollowing(convertServer, sql);
	}
}

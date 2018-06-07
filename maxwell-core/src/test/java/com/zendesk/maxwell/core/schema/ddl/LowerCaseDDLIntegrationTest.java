package com.zendesk.maxwell.core.schema.ddl;

import com.zendesk.maxwell.test.mysql.MysqlIsolatedServer;
import com.zendesk.maxwell.core.SpringTestContextConfiguration;
import com.zendesk.maxwell.core.support.MaxwellTestSupport;
import com.zendesk.maxwell.test.mysql.MysqlIsolatedServerSupport;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { SpringTestContextConfiguration.class })
public class LowerCaseDDLIntegrationTest {
	private static MysqlIsolatedServer convertServer;
	private static MysqlIsolatedServer caseSensitiveServer;
	private static Boolean caseSensitive = null;

	@Autowired
	private MaxwellTestSupport maxwellTestSupport;
	@Autowired
	private MysqlIsolatedServerSupport mysqlIsolatedServerSupport;

	@Before
	public void setupServers() throws Exception {
		if(convertServer == null) {
			convertServer = mysqlIsolatedServerSupport.setupServer("--lower-case-table-names=1");
			mysqlIsolatedServerSupport.setupSchema(convertServer);
		}

		if ( caseSensitiveServer == null && isFileSystemCaseSensitive() ) {
			caseSensitiveServer = mysqlIsolatedServerSupport.setupServer("--lower-case-table-names=0");
			mysqlIsolatedServerSupport.setupSchema(caseSensitiveServer);
		}
	}


	public boolean isFileSystemCaseSensitive() throws Exception {
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

		maxwellTestSupport.testDDLFollowing(convertServer, sql);
	}

	@Test
	public void TestLowerCasingTableRename() throws Exception {
		String sql[] = {
			"create TABLE ttt( a int )",
			"rename table ttt to TTTT",
			"create table ttRR like tttt"
		};

		maxwellTestSupport.testDDLFollowing(convertServer, sql);
	}
}

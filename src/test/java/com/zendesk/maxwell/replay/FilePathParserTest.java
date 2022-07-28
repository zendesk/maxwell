package com.zendesk.maxwell.replay;

import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertFalse;

/**
 * @author udyr@shlaji.com
 */
public class FilePathParserTest {

	@Test
	public void testParsePath() {
		String path = "/test/^test$,/ttmp,/tmp/maxwell.log,C:\\Windows\\explorer.exe,logs/^*.log,D:/tmp/mysql.000356";
		List<ReplayFilePattern> patterns = FilePathParser.parse(path);
		assertFalse(patterns.isEmpty());

		for (ReplayFilePattern pattern : patterns) {
			System.out.printf("\nfound parttern: %s%s\n", pattern.getBasePath(), pattern.getFilePattern() == null ? "" : pattern.getFilePattern().pattern());

			List<File> files = pattern.getExistFiles();
			for (File file : files) {
				System.out.printf("found files: %s\n", file.getAbsoluteFile());
			}
		}
	}
}

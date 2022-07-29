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
	public void testSinglePath() {
		String path;
		List<ReplayFilePattern> patterns;

		path = "logs/^*.log";
		patterns = FilePathParser.parse(path);
		assertFalse(patterns.isEmpty());
		printPath(patterns);

		path = "/test/^test$";
		patterns = FilePathParser.parse(path);
		assertFalse(patterns.isEmpty());
		printPath(patterns);

		path = "/test/test";
		patterns = FilePathParser.parse(path);
		assertFalse(patterns.isEmpty());
		printPath(patterns);

		path = "^*$";
		patterns = FilePathParser.parse(path);
		assertFalse(patterns.isEmpty());
		printPath(patterns);

		path = "/replay/^bin.000[3-4][0-9][0-9]$";
		patterns = FilePathParser.parse(path);
		assertFalse(patterns.isEmpty());
		printPath(patterns);
	}

	@Test
	public void testMultiPath() {
		String path = "/test/^test$,/ttmp,/tmp/maxwell.log,C:\\Windows\\explorer.exe,logs/^*.log,D:/tmp/mysql.000356";
		List<ReplayFilePattern> patterns = FilePathParser.parse(path);
		assertFalse(patterns.isEmpty());
		printPath(patterns);
	}

	private void printPath(List<ReplayFilePattern> patterns) {
		for (ReplayFilePattern pattern : patterns) {
			System.out.printf("\nfound parttern: %s%s\n", pattern.getBasePath(), pattern.getFilePattern() == null ? "" : pattern.getFilePattern().pattern());

			List<File> files = pattern.getExistFiles();
			for (File file : files) {
				System.out.printf("found files: %s\n", file.getAbsoluteFile());
			}
		}
	}
}

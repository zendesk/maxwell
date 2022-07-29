package com.zendesk.maxwell.replay;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author udyr@shlaji.com
 */
public class ReplayFilePattern {

	/**
	 * If empty, basePath must be a binlog file
	 */
	private final Pattern filePattern;

	/**
	 * A base directory or file
	 */
	private final String basePath;

	public ReplayFilePattern(String basePath, Pattern filePattern) {
		this.filePattern = filePattern;
		this.basePath = basePath;
	}

	/**
	 * Matches valid files in the base path
	 *
	 * @return files
	 */
	public List<File> getExistFiles() {
		File baseFile = new File(basePath);
		if (!baseFile.exists()) {
			return Collections.emptyList();
		}

		if (Objects.isNull(filePattern)) {
			return baseFile.isFile() ? Collections.singletonList(baseFile) : Collections.emptyList();
		}

		File[] fileArray = baseFile.listFiles();
		if (Objects.isNull(fileArray)) {
			return Collections.emptyList();
		}
		return Arrays.stream(fileArray).filter(f -> f.exists() && f.isFile() && filePattern.matcher(f.getName()).find()).collect(Collectors.toList());
	}

	public String getBasePath() {
		return basePath;
	}

	public Pattern getFilePattern() {
		return filePattern;
	}
}

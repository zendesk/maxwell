package com.zendesk.maxwell.replay;

import java.io.File;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author udyr@shlaji.com
 */
public class ReplayFilePattern {
	private final Pattern filePattern;
	private final String basePath;

	public ReplayFilePattern(String basePath, Pattern filePattern) {
		this.filePattern = filePattern;
		this.basePath = basePath;
	}

	public List<File> getExistFiles() {
		File fileDir = new File(basePath);
		if (!fileDir.exists()) {
			return new ArrayList<>();
		}

		if (filePattern == null) {
			return Collections.singletonList(fileDir);
		}

		File[] fileArray = fileDir.listFiles();
		if (Objects.isNull(fileArray)) {
			return new ArrayList<>();
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

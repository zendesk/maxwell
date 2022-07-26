package com.zendesk.maxwell;

/**
 * Describes the 3 difference case sensitivity settings on a Mysql server.
 * Case sensitivity rules apply to databases and tables, not columns.
 */
public enum CaseSensitivity { CASE_SENSITIVE, CONVERT_TO_LOWER, CONVERT_ON_COMPARE };



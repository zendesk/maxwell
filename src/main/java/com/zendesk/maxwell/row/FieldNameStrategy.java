package com.zendesk.maxwell.row;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 
 * @author frankchen Dec 24, 2019 9:08:41 PM
 */
public class FieldNameStrategy {

	public final static String UNDERSCORE_TO_CAMEL_CASE = "underscore_to_camelcase";

	// cache converted names
	private Map<String, String> caches = new ConcurrentHashMap<>();

	private String strategyName;

	public FieldNameStrategy(String strategy) {
		this.strategyName = strategy;
	}
	
	public String apply(String oldName) {
		if ( UNDERSCORE_TO_CAMEL_CASE.equals(strategyName)) 
			return caches.computeIfAbsent(oldName, k -> underscore2Camel(k));
		
		return oldName;
	}
	
	private String underscore2Camel(String oldName) {
		StringBuilder newName = new StringBuilder(oldName.length());
		for (int i = 0, len = oldName.length(); i < len; i++) {
			char c = oldName.charAt(i);
			if (c == '_' ) {
				if ( newName.length() > 0 //ignore the leading underscore 
				  && i + 1 < len //this underscore is not the last character 
				  ) {
					char afterUnderscore = oldName.charAt(++i);
					
					newName.append(Character.isLowerCase(afterUnderscore)	? (char) (afterUnderscore - 'a' + 'A')
																			: afterUnderscore);
				}
			} else {
				newName.append(c);
			}
		}
		return newName.toString();
	}
}

package com.zendesk.maxwell.row;

import com.github.shyiko.mysql.binlog.event.LRUCache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 
 * @author frankchen Dec 24, 2019 9:08:41 PM
 */
public class FieldNameStrategy {

	public final static int TYPE_NONE = 0;

	public final static String NAME_UNDERSCORE_TO_CAMEL_CASE = "underscore_to_camelcase";
	public final static int TYPE_UNDERSCORE_TO_CAMEL_CASE = 1;
	
	// cache converted names
	private Map<String, String> caches = new LRUCache<>(20, 0.75f, 1000);

	private int strategyType = TYPE_NONE;

	public FieldNameStrategy(String strategyName) {
		if ( NAME_UNDERSCORE_TO_CAMEL_CASE.equals(strategyName) )
			strategyType = TYPE_UNDERSCORE_TO_CAMEL_CASE;
	}
	
	public String apply(String oldName) {
		if ( strategyType == TYPE_UNDERSCORE_TO_CAMEL_CASE ) 
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
					char ch = oldName.charAt(++i);
					
					//convert lower case char after underscore to upper one
					//or leave it whatever it is
					if ( Character.isLowerCase(ch) )
						ch = Character.toUpperCase(ch);

					newName.append(ch);
				}
			} else {
				newName.append(c);
			}
		}
		
		//extreme case, input may be all underscore chars
		if ( newName.length() == 0 )
			return oldName;
		
		return newName.toString();
	}
}

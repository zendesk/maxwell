package com.zendesk.maxwell.row.naming;

/**
 * 
 * @author frankchen
 * @Date Dec 24, 2019 8:51:37 PM
 */
public interface INamingStrategy {
    
    String apply(String oldName);
    
}

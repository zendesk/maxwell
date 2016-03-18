package com.zendesk.maxwell.filter;

import java.io.File;
import java.io.FileInputStream;
import java.util.Collection;

import org.yaml.snakeyaml.Yaml;

import com.zendesk.maxwell.RowMap;
import com.zendesk.maxwell.config.FilterColumns;
import com.zendesk.maxwell.config.FilterConfig;

public class MaxwellColumnFilter {
    Collection<FilterConfig> filters = null;

    public MaxwellColumnFilter(String path) {
        if(path!=null){
            try (FileInputStream fileInputStream = new FileInputStream(new File(path))) {
                Yaml yaml = new Yaml();
                filters = (Collection<FilterConfig>) yaml.load(fileInputStream);
                System.out.println(filters.size());
            } catch (Exception e) {
                e.printStackTrace();
                
            }
        }
    }

    public void applyFilter(RowMap r) {
       if (filters != null) {
            for (FilterConfig filterConfig: filters) {
                if (filterConfig.getTable().equalsIgnoreCase(r.getTable())) {
                    for(FilterColumns col:filterConfig.getColumns()){
                        r.removeData(col.getColumn());
                        r.removeOldData(col.getColumn());
                    }
                }
            }
           
        }

    }
}

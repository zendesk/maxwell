package com.zendesk.maxwell.config;

import java.util.List;

public class FilterConfig {
    public String table;
    public List<FilterColumns> columns;
    
    public List<FilterColumns> getColumns() {
        return columns;
    }

    public void setColumns(List<FilterColumns> columns) {
        this.columns = columns;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

}



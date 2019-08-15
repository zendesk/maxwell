package com.zendesk.maxwell.producer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowIdentity;
import com.zendesk.maxwell.row.RowMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerProducer extends AbstractProducer {
    public static final Logger LOGGER = LoggerFactory.getLogger(SpannerProducer.class);

    private final DatabaseClient dbClient;

    private String sourceDatabase;

    public SpannerProducer(MaxwellContext context, String project, String instance, String database, String sourceDatabase) throws IOException {
        super(context);
        SpannerOptions options = SpannerOptions.newBuilder().build();
        Spanner spanner = options.getService();
        DatabaseId db = DatabaseId.of(project, instance, database);
        this.dbClient = spanner.getDatabaseClient(db);
        this.sourceDatabase = sourceDatabase;
        LOGGER.info("Spanner Producer initialized with client" + this.dbClient.toString() + ", Project: "+ project);
    }

    public void manipulate(DatabaseClient dbClient, String sql) {
        dbClient
            .readWriteTransaction()
            .run(new TransactionCallable<Void>() {
                public Void run(TransactionContext tx) throws Exception {
                    List<Statement> stmts = new ArrayList<Statement>();
                    stmts.add(Statement.newBuilder(sql).build());
                    tx.batchUpdate(stmts);
                    return null;
                }
            });
    }

    @Override
    public void push(RowMap r) throws Exception {
        try {
            String db = r.getDatabase();
            if (!db.equalsIgnoreCase(sourceDatabase)) {
                LOGGER.info("Replication not whitelisted for DB: " + db);
                return;
            }
            String table = r.getTable();
            String type = r.getRowType();
            LOGGER.info("Table:" + table);
            LinkedHashMap<String, Object> data = r.getData();
            String sql = "";
            if (type == "insert" || type == "replace") {
                sql = this.getInsertSql(r);
            } else if (type == "update") {
                sql = this.getUpdateSql(r);
            } else if (type == "delete") {
                sql = this.getDeleteSql(r);
            } else {
                sql = r.getRowQuery();
            }

            LOGGER.info("SQL:" + sql);
            this.manipulate(dbClient, sql);

            this.context.setPosition(r);
        } catch (Exception e) {
            LOGGER.info("Error: " + e.getMessage() + ", Data: " + r.toJSON(outputConfig));
        }
    }

    private String getInsertSql(RowMap r) {
        String table = r.getTable();
        LinkedHashMap<String, Object> data = r.getData();
        String sql = "";
        sql = "INSERT INTO " + table + "(";
        Iterator<String> keys = data.keySet().iterator();
        while (keys.hasNext()) {
            sql += keys.next() + ",";
        }
        if (sql.endsWith(",")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        sql += ") VALUES (";
        Iterator<Object> values = data.values().iterator();
        while (values.hasNext()) {
            Object nextObject = values.next();
            if (nextObject instanceof Long) {
                sql += (Long) nextObject + ",";
            } else if (nextObject instanceof Integer) {
                sql += (String) nextObject + ",";
            } else if (nextObject instanceof String) {
                sql += "'" + (String) nextObject + "'" + ",";
            } else {
                sql += (String) nextObject + ",";
            }
        }
        if (sql.endsWith(",")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        sql += ")";
        return sql;
    }

    private String getUpdateSql(RowMap r) {
        String table = r.getTable();
        LinkedHashMap<String, Object> data = r.getData();
        LinkedHashMap<String, Object> oldData = r.getOldData();
        String sql = "";
        sql = "UPDATE " + table + " SET ";
        Iterator<String> oldKeys = oldData.keySet().iterator();
        while (oldKeys.hasNext()) {
            String oldKey = oldKeys.next();
            sql += oldKey + " = ";
            Object valueObject = data.get(oldKey);
            if (valueObject instanceof Long) {
                sql += (Long) valueObject + ",";
            } else if (valueObject instanceof Integer) {
                sql += (String) valueObject + ",";
            } else if (valueObject instanceof String) {
                sql += "'" + (String) valueObject + "'" + ",";
            } else {
                sql += (String) valueObject + ",";
            }
        }
        if (sql.endsWith(",")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        sql += " WHERE ";
        for (String pkColumn : r.getPKColumns()) {
            sql += pkColumn + " = ";
            Object valueObject = data.get(pkColumn);
            if (valueObject instanceof Long) {
                sql += (Long) valueObject + " AND ";
            } else if (valueObject instanceof Integer) {
                sql += (String) valueObject + " AND ";
            } else if (valueObject instanceof String) {
                sql += "'" + (String) valueObject + "'" + " AND ";
            } else {
                sql += (String) valueObject + " AND ";
            }
        }
        if (sql.endsWith(" AND ")) {
            sql = sql.substring(0, sql.length() - 5);
        }
        return sql;
    }

    private String getDeleteSql(RowMap r) {
        String table = r.getTable();
        LinkedHashMap<String, Object> data = r.getData();
        LinkedHashMap<String, Object> oldData = r.getOldData();
        String sql = "";
        sql = "DELETE FROM " + table + " WHERE ";
        for (String pkColumn : r.getPKColumns()) {
            sql += pkColumn + " = ";
            Object valueObject = data.get(pkColumn);
            if (valueObject instanceof Long) {
                sql += (Long) valueObject + " AND ";
            } else if (valueObject instanceof Integer) {
                sql += (String) valueObject + " AND ";
            } else if (valueObject instanceof String) {
                sql += "'" + (String) valueObject + "'" + " AND ";
            } else {
                sql += (String) valueObject + " AND ";
            }
        }
        if (sql.endsWith(" AND ")) {
            sql = sql.substring(0, sql.length() - 5);
        }
        return sql;
    }
}

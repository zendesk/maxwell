package com.zendesk.maxwell.test.mysql;

import com.zendesk.maxwell.api.schema.SchemaStoreSchema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.file.Path;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Service
public class MysqlIsolatedServerSupport {
    @Autowired
    private SchemaStoreSchema schemaStoreSchema;
    @Autowired
    private MysqlTestData mysqlTestData;


    public MysqlIsolatedServer setupServer(String extraParams) throws Exception {
        MysqlIsolatedServer server = new MysqlIsolatedServer();
        server.boot(extraParams);

        Connection conn = server.getConnection();
        schemaStoreSchema.ensureMaxwellSchema(conn, "maxwell");
        conn.createStatement().executeQuery("use maxwell");
        schemaStoreSchema.upgradeSchemaStoreSchema(conn);
        return server;
    }

    public MysqlIsolatedServer setupServer() throws Exception {
        return setupServer(null);
    }

    public void setupSchema(MysqlIsolatedServer server, boolean resetBinlogs) throws Exception {
        List<String> queries = new ArrayList<String>(Arrays.asList(
                "CREATE DATABASE if not exists shard_2",
                "DROP DATABASE if exists shard_1",
                "CREATE DATABASE shard_1",
                "USE shard_1"
        ));

        for (SqlFile file : mysqlTestData.getSqlFilesOfFolder("schema", this::isSqlFileAndIsNotSharded)) {
            String s = new String(file.getData());
            if (s != null) {
                queries.add(s);
            }
        }

        String shardedFileName;
        if (server.getVersion().atLeast(MysqlIsolatedServer.VERSION_5_6)){
            shardedFileName = "sharded.sql";
        } else {
            shardedFileName = "sharded_55.sql";
        }

        SqlFile sql = mysqlTestData.getSqlFile("schema/" + shardedFileName);
        String s = new String(sql.getData());
        if (s != null) {
            queries.add(s);
        }

        if (resetBinlogs)
            queries.add("RESET MASTER");

        server.executeList(queries);
    }

    private boolean isSqlFileAndIsNotSharded(Path p){
        String name = p.getFileName().toString();
        return name.endsWith(".sql") && !name.contains("sharded");
    }

    public void setupSchema(MysqlIsolatedServer server) throws Exception {
        setupSchema(server, true);
    }

}

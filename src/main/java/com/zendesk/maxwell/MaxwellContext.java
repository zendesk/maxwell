package com.zendesk.maxwell;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.bootstrap.AbstractBootstrapper;
import com.zendesk.maxwell.bootstrap.AsynchronousBootstrapper;
import com.zendesk.maxwell.bootstrap.NoOpBootstrapper;
import com.zendesk.maxwell.bootstrap.SynchronousBootstrapper;
import com.zendesk.maxwell.filter.MaxwellColumnFilter;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.producer.FileProducer;
import com.zendesk.maxwell.producer.MaxwellFilteringProducer;
import com.zendesk.maxwell.producer.MaxwellKafkaProducer;
import com.zendesk.maxwell.producer.ProfilerProducer;
import com.zendesk.maxwell.producer.StdoutProducer;
import com.zendesk.maxwell.schema.ReadOnlySchemaPosition;
import com.zendesk.maxwell.schema.SchemaPosition;
import com.zendesk.maxwell.schema.SchemaScavenger;

import snaq.db.ConnectionPool;

public class MaxwellContext {
    static final Logger LOGGER = LoggerFactory.getLogger(MaxwellContext.class);

    private final ConnectionPool replicationConnectionPool;
    private final ConnectionPool maxwellConnectionPool;
    private final MaxwellConfig config;
    private SchemaPosition schemaPosition;
    private Long serverID;
    private BinlogPosition initialPosition;
    private CaseSensitivity caseSensitivity;

    public MaxwellContext(MaxwellConfig config) {
        this.config = config;

        this.replicationConnectionPool = new ConnectionPool("ReplicationConnectionPool", 10, 0, 10,
                config.replicationMysql.getConnectionURI(), config.replicationMysql.user,
                config.replicationMysql.password);

        this.maxwellConnectionPool = new ConnectionPool("MaxwellConnectionPool", 10, 0, 10,
                config.maxwellMysql.getConnectionURI(), config.maxwellMysql.user, config.maxwellMysql.password);
        this.maxwellConnectionPool.setCaching(false);

        if (this.config.initPosition != null)
            this.initialPosition = this.config.initPosition;
    }

    public MaxwellConfig getConfig() {
        return this.config;
    }

    public ConnectionPool getReplicationConnectionPool() {
        return this.replicationConnectionPool;
    }

    public ConnectionPool getMaxwellConnectionPool() {
        return this.maxwellConnectionPool;
    }

    public Connection getMaxwellConnection() throws SQLException {
        Connection conn = this.maxwellConnectionPool.getConnection();
        conn.setCatalog(config.databaseName);
        return conn;
    }

    public void start() {
        SchemaScavenger s = new SchemaScavenger(this.maxwellConnectionPool, this.config.databaseName);
        new Thread(s).start();
    }

    public void terminate() {
        if (this.schemaPosition != null) {
            try {
                this.schemaPosition.stopLoop();
            } catch (TimeoutException e) {
                LOGGER.error("got timeout trying to shutdown schemaPosition thread.");
            }
        }
        this.replicationConnectionPool.release();
        this.maxwellConnectionPool.release();
    }

    private SchemaPosition getSchemaPosition() throws SQLException {
        if (this.schemaPosition == null) {
            if (this.getConfig().replayMode) {
                this.schemaPosition = new ReadOnlySchemaPosition(this.getMaxwellConnectionPool(), this.getServerID(),
                        this.config.databaseName);
            } else {
                this.schemaPosition = new SchemaPosition(this.getMaxwellConnectionPool(), this.getServerID(),
                        this.config.databaseName);
            }

            this.schemaPosition.start();
        }
        return this.schemaPosition;
    }

    public BinlogPosition getInitialPosition() throws SQLException {
        if (this.initialPosition != null)
            return this.initialPosition;

        this.initialPosition = getSchemaPosition().get();
        return this.initialPosition;
    }

    public void setPosition(RowMap r) throws SQLException {
        if (r.isTXCommit())
            this.setPosition(r.getPosition());
    }

    public void setPosition(BinlogPosition position) throws SQLException {
        this.getSchemaPosition().set(position);
    }

    public void setPositionSync(BinlogPosition position) throws SQLException {
        this.getSchemaPosition().setSync(position);
    }

    public void ensurePositionThread() throws SQLException {
        if (this.schemaPosition == null)
            return;

        SQLException e = this.schemaPosition.getException();
        if (e != null) {
            throw (e);
        }
    }

    public Long getServerID() throws SQLException {
        if (this.serverID != null)
            return this.serverID;

        try (Connection c = getReplicationConnectionPool().getConnection()) {
            ResultSet rs = c.createStatement().executeQuery("SELECT @@server_id as server_id");
            if (!rs.next()) {
                throw new RuntimeException("Could not retrieve server_id!");
            }
            this.serverID = rs.getLong("server_id");
            return this.serverID;
        }
    }

    public CaseSensitivity getCaseSensitivity() throws SQLException {
        if (this.caseSensitivity != null)
            return this.caseSensitivity;

        try (Connection c = getReplicationConnectionPool().getConnection()) {
            ResultSet rs = c.createStatement().executeQuery("select @@lower_case_table_names");
            if (!rs.next())
                throw new RuntimeException("Could not retrieve @@lower_case_table_names!");

            int value = rs.getInt(1);
            switch (value) {
            case 0:
                this.caseSensitivity = CaseSensitivity.CASE_SENSITIVE;
                break;
            case 1:
                this.caseSensitivity = CaseSensitivity.CONVERT_TO_LOWER;
                break;
            case 2:
                this.caseSensitivity = CaseSensitivity.CONVERT_ON_COMPARE;
                break;
            default:
                throw new RuntimeException("Unknown value for @@lower_case_table_names: " + value);
            }
            return this.caseSensitivity;
        }
    }

    public AbstractProducer getProducer() throws IOException {
        AbstractProducer producer;
        switch (this.config.producerType) {
        case "file":
            producer = new FileProducer(this, this.config.outputFile);
            break;
        case "kafka":
            producer = new MaxwellKafkaProducer(this, this.config.getKafkaProperties(), this.config.kafkaTopic);
            break;
        case "profiler":
            producer = new ProfilerProducer(this);
            break;
        case "stdout":
        default:
            producer = new StdoutProducer(this);
        }
        if (this.config.isFilterRequired) {
            producer = new MaxwellFilteringProducer(this, producer);
        }
        return producer;
    }

    public AbstractBootstrapper getBootstrapper() throws IOException {
        switch (this.config.bootstrapperType) {
        case "async":
            return new AsynchronousBootstrapper(this);
        case "sync":
            return new SynchronousBootstrapper(this);
        default:
            return new NoOpBootstrapper(this);
        }

    }

    public MaxwellFilter buildFilter() throws MaxwellInvalidFilterException {
        return new MaxwellFilter(config.includeDatabases, config.excludeDatabases, config.includeTables,
                config.excludeTables, config.blacklistDatabases, config.blacklistTables);
    }

    public boolean getReplayMode() {
        return this.config.replayMode;
    }

    private void probePool(ConnectionPool pool, String uri) throws SQLException {
        try (Connection c = pool.getConnection()) {
            c.createStatement().execute("SELECT 1");
        } catch (SQLException e) {
            LOGGER.error("Could not connect to " + uri + ": " + e.getLocalizedMessage());
            throw (e);
        }
    }

    public void probeConnections() throws SQLException {
        probePool(this.maxwellConnectionPool, this.config.maxwellMysql.getConnectionURI());

        if (this.maxwellConnectionPool != this.replicationConnectionPool)
            probePool(this.replicationConnectionPool, this.config.replicationMysql.getConnectionURI());
    }
}

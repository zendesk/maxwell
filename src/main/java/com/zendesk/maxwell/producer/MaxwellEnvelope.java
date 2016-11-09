package com.zendesk.maxwell.producer;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

public class MaxwellEnvelope extends SpecificRecordBase implements SpecificRecord {
    static final Logger LOGGER = LoggerFactory.getLogger(MaxwellEnvelope.class);

    public static Schema SCHEMA = null;

    private static final InputStream avroSchema = MaxwellEnvelope.class.getResourceAsStream("/avro/MaxwellEnvelope.avro");

    private CharSequence database;
    private CharSequence table;
    private CharSequence type;
    private Long ts;
    private Long xid;
    private Boolean commit;
    private CharSequence position;
    private Long serverId;
    private Long threadId;
    private CharSequence data;
    private CharSequence old;

    public Schema getSchema() {
        if (SCHEMA == null) {
            try {
                SCHEMA = new Schema.Parser().parse(avroSchema);
            } catch (java.io.IOException ioe) {
                LOGGER.error("File '" + avroSchema + "' name does not exist. Exception: " + ioe.getLocalizedMessage());
                return null;
            }
        }
        return SCHEMA;
    }

    public MaxwellEnvelope(CharSequence database, CharSequence table, CharSequence type, Long ts, Long xid, Boolean commit, CharSequence position, Long serverId, Long threadId, CharSequence data, CharSequence old) {
        this.database = database;
        this.table = table;
        this.type = type;
        this.ts = ts;
        this.xid = xid;
        this.commit = commit;
        this.position = position;
        this.serverId = serverId;
        this.threadId = threadId;
        this.data = data;
        this.old = old;
    }

    // Used by DatumWriter.  Applications should not call.
    public java.lang.Object get(int field) {
        switch (field) {
            case 0:
                return this.database;
            case 1:
                return this.table;
            case 2:
                return this.type;
            case 3:
                return this.ts;
            case 4:
                return this.xid;
            case 5:
                return this.commit;
            case 6:
                return this.position;
            case 7:
                return this.serverId;
            case 8:
                return this.threadId;
            case 9:
                return this.data;
            case 10:
                return this.old;
            default:
                throw new AvroRuntimeException("Bad index found in get(), index is " + field);
        }
    }

    // Used by DatumReader.  Applications should not call.
    @SuppressWarnings(value = "unchecked")
    public void put(int field, java.lang.Object value) {
        switch (field) {
            case 0:
                this.database = (CharSequence) value;
                break;
            case 1:
                this.table = (CharSequence) value;
                break;
            case 2:
                this.type = (CharSequence) value;
                break;
            case 3:
                this.ts = (Long) value;
                break;
            case 4:
                this.xid = (Long) value;
                break;
            case 5:
                this.commit = (Boolean) value;
                break;
            case 6:
                this.position = (CharSequence) value;
                break;
            case 7:
                this.serverId = (Long) value;
                break;
            case 8:
                this.threadId = (Long) value;
                break;
            case 9:
                this.data = (CharSequence) value;
                break;
            case 10:
                this.old = (CharSequence) value;
                break;
            default:
                throw new AvroRuntimeException("Bad index");
        }
    }

    @Override
    public String toString() {
        return "MaxwellEnvelope{" +
                "database=" + database +
                ", table=" + table +
                ", type=" + type +
                ", ts=" + ts +
                ", xid=" + xid +
                ", commit=" + commit +
                ", position=" + position +
                ", serverId=" + serverId +
                ", threadId=" + threadId +
                ", data=" + data +
                ", old=" + old +
                '}';
    }
}
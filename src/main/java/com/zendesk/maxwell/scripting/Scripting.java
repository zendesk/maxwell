package com.zendesk.maxwell.scripting;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashMap;

import com.zendesk.maxwell.row.HeartbeatRowMap;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.ddl.DDLMap;
import org.openjdk.nashorn.api.scripting.ScriptObjectMirror;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Scripting {
	static final Logger LOGGER = LoggerFactory.getLogger(Scripting.class);

	private final ScriptObjectMirror processRowFunc, processHeartbeatFunc, processDDLFunc;

	private LinkedHashMap<String, Object> globalJavascriptState;

	private ScriptObjectMirror getFunc(ScriptEngine engine, String fName, String filename) {
		ScriptObjectMirror f = (ScriptObjectMirror) engine.get(fName);
		if ( f == null )
			return null;
		else if ( !f.isFunction() ) {
			throw new RuntimeException("Expected " + fName + " to be a function!");
		} else {
			LOGGER.info("using function " + fName + " from " + filename);
		}
		return f;
	}

	public Scripting(String filename) throws IOException, ScriptException, NoSuchMethodException {
		ScriptEngineManager manager = new ScriptEngineManager();
		ScriptEngine engine = manager.getEngineByName("nashorn");

		String externJS = new String(Files.readAllBytes(Paths.get(filename)));
		engine.put("logger", LOGGER);
		engine.eval(externJS);

		processRowFunc = getFunc(engine, "process_row", filename);
		processHeartbeatFunc = getFunc(engine, "process_heartbeat", filename);
		processDDLFunc = getFunc(engine, "process_ddl", filename);

		globalJavascriptState = new LinkedHashMap<String, Object>();

		if ( processRowFunc == null && processHeartbeatFunc == null && processDDLFunc == null )
			LOGGER.warn("expected " + filename + " to define at least one of: process_row,process_heartbeat,process_ddl");
	}

	public void invoke(RowMap row) {
		if ( row instanceof HeartbeatRowMap && processHeartbeatFunc != null )
			processHeartbeatFunc.call(null, new WrappedHeartbeatMap((HeartbeatRowMap) row), globalJavascriptState);
		else if ( row instanceof DDLMap && processDDLFunc != null )
			processDDLFunc.call(null, new WrappedDDLMap((DDLMap) row), globalJavascriptState);
		else if ( row instanceof RowMap && processRowFunc != null )
			processRowFunc.call(null, new WrappedRowMap(row), globalJavascriptState);
	}

	private static ThreadLocal<ScriptEngine> stringifyEngineThreadLocal = ThreadLocal.withInitial(() -> {
		ScriptEngineManager manager = new ScriptEngineManager();
		return manager.getEngineByName("nashorn");
	});

	public static String stringify(ScriptObjectMirror mirror) throws ScriptException {
		ScriptObjectMirror json = (ScriptObjectMirror) stringifyEngineThreadLocal.get().eval("JSON");
		return (String) json.callMember("stringify", mirror);
	}
}

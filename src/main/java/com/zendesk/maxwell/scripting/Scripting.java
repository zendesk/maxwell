package com.zendesk.maxwell.scripting;

import javax.script.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.zendesk.maxwell.row.HeartbeatRowMap;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.ddl.DDLMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Scripting {
	static final Logger LOGGER = LoggerFactory.getLogger(Scripting.class);
	final ScriptEngine engine;

	boolean has_process_row = true;
	boolean has_process_ddl = true;
	boolean has_process_heartbeat = true;

	public Scripting(String filename) throws IOException, ScriptException, NoSuchMethodException {
		ScriptEngineManager manager = new ScriptEngineManager();

		engine = manager.getEngineByName("graal.js");
		Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
		bindings.put("polyglot.js.allowHostAccess", true);
		bindings.put("polyglot.js.nashorn-compat", true);

		String externJS = new String(Files.readAllBytes(Paths.get(filename)));
		LOGGER.info("reading javascript filter: " + filename);
		engine.put("logger", LOGGER);
		engine.eval(externJS);
	}

	public void invokeFunction(String name, Object row) throws ScriptException, NoSuchMethodException {
		((Invocable) engine).invokeFunction(name, row);
	}

	public void invokeProcessHeartbeat(Object row) throws ScriptException {
		try {
			invokeFunction("process_heartbeat", row);
		} catch (NoSuchMethodException e) {
			has_process_heartbeat = false;
		}
	}

	public void invokeProcessDDL(Object row) throws ScriptException {
		try {
			invokeFunction("process_ddl", row);
		} catch (NoSuchMethodException e) {
			has_process_ddl = false;
		}
	}

	public void invokeProcessRow(Object row) throws ScriptException {
		try {
			invokeFunction("process_row", row);
		} catch (NoSuchMethodException e) {
			has_process_row = false;
		}
	}

	public void invoke(RowMap row) throws ScriptException {
		if ( row instanceof HeartbeatRowMap && has_process_heartbeat )
			invokeProcessHeartbeat(new WrappedHeartbeatMap((HeartbeatRowMap) row));
		else if ( row instanceof DDLMap && has_process_ddl )
			invokeProcessDDL(new WrappedDDLMap((DDLMap) row));
		else if ( row instanceof RowMap && has_process_row )
			invokeProcessRow(new WrappedRowMap(row));
	}

	private static ThreadLocal<ScriptEngine> stringifyEngineThreadLocal = ThreadLocal.withInitial(() -> {
		ScriptEngineManager manager = new ScriptEngineManager();
		return manager.getEngineByName("graal.js");
	});

	public static String stringify(Object mirror) throws ScriptException, NoSuchMethodException {
		ScriptEngine engine = stringifyEngineThreadLocal.get();
		return (String) ((Invocable)engine).invokeFunction("JSON.stringify", mirror);
	}
}

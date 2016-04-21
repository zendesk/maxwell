package com.zendesk.maxwell;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.InetSocketAddress;

public class MaxwellState {
    private static MaxwellState ourInstance = new MaxwellState();
    private static final JsonFactory jsonFactory = new JsonFactory();
    private HttpServer statusServer = null;

    private String runState = "NOT_STARTED";
    private long currentPosition;

    public String getRunState() {
        return runState.toString();
    }

    public void setRunState(String runState) {
        this.runState = runState;
    }

    public long getCurrentPosition() {
        return currentPosition;
    }

    public void setCurrentPosition(long currentPosition) {
        this.currentPosition = currentPosition;
    }

    public static MaxwellState getInstance() {
        return ourInstance;
    }

    private MaxwellState() {
    }

    public synchronized void startServer(Integer statusPort) throws IOException {
        if (statusServer == null) {
            statusServer = HttpServer.create(new InetSocketAddress(statusPort), 0);
            System.out.println("statusPort = " + statusPort);
            statusServer.createContext("/state", new StateHttpHandler());
            statusServer.setExecutor(null); // creates a default executor
            statusServer.start();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    statusServer.stop(0);
                }
            });
        }
    }

    private class StateHttpHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange t) throws IOException {
            int responseCode = "RUNNING".equals(runState) ? 200 : 404;
            String response = createResponseJson();
            System.out.println("response = " + response);
            t.sendResponseHeaders(responseCode, response.length());
            OutputStream os = t.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }

        private String createResponseJson() throws IOException {
            StringWriter responseWriter = new StringWriter();
            JsonGenerator generator = jsonFactory.createGenerator(responseWriter);
            generator.writeStartObject();
            generator.writeStringField("runState", getRunState());
            generator.writeNumberField("currentPosition", getCurrentPosition());
            generator.writeEndObject();
            generator.close();
            return responseWriter.toString();
        }
    }
}

package com.zendesk.maxwell.monitoring;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.filtering.InvalidFilterException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.stream.Collectors;

/**
 * An HTTP servlet which allows for the live reconfiguration of maxwell. If
 * property provided for update is null, it will be ignored.
 */
public class MaxwellConfigServlet extends HttpServlet {
	private static final java.lang.String CONTENT_TYPE = "application/json";
	private static final ObjectMapper mapper = new ObjectMapper();
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellConfigServlet.class);
	private final MaxwellContext context;

	public MaxwellConfigServlet(MaxwellContext context) {
		super();
		this.context = context;
	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	private static class SerializedConfig {
		public String filter;
	}

	private static class ErrorResponse {
		public int code;
		public String message;

		public ErrorResponse(int code, String message) {
			this.code = code;
			this.message = message;
		}

		public static String getErrorResponseString(int code, String message) throws JsonProcessingException {
			ErrorResponse errResponse = new ErrorResponse(code, message);
			return String.format("{\"error\":%s}", mapper.writeValueAsString(errResponse));
		}
	}

	private void handleConfigUpdateRequest(HttpServletRequest req, HttpServletResponse resp) throws IOException {
		SerializedConfig sconfig;
		try {
			String reqbody = req.getReader().lines().collect(Collectors.joining(System.lineSeparator()));
			sconfig = mapper.readValue(reqbody, SerializedConfig.class);
		} catch (Exception e) {
			resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			resp.setContentType(CONTENT_TYPE);
			try (PrintWriter writer = resp.getWriter()) {
				writer.println(ErrorResponse.getErrorResponseString(HttpServletResponse.SC_BAD_REQUEST,
						String.format("error processing body: %s", e.getMessage())));
			}
			return;
		}

		try {
			applySerializedConfig(sconfig);

			resp.setStatus(HttpServletResponse.SC_OK);
			resp.setContentType(CONTENT_TYPE);
			try (PrintWriter writer = resp.getWriter()) {
				writer.println(getSerializedConfig());
			}
		} catch (InvalidFilterException e) {
			resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			resp.setContentType(CONTENT_TYPE);
			try (PrintWriter writer = resp.getWriter()) {
				writer.println(ErrorResponse.getErrorResponseString(HttpServletResponse.SC_BAD_REQUEST,
						String.format("invalid filter: %s", e.getMessage())));
			}
		}
	}

	@Override
	protected void doPut(HttpServletRequest req, HttpServletResponse resp) throws IOException {
		this.handleConfigUpdateRequest(req, resp);
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
		this.handleConfigUpdateRequest(req, resp);
	}

	protected void doPatch(HttpServletRequest req, HttpServletResponse resp) throws IOException {
		this.handleConfigUpdateRequest(req, resp);
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
		resp.setStatus(HttpServletResponse.SC_OK);
		resp.setContentType(CONTENT_TYPE);
		try (PrintWriter writer = resp.getWriter()) {
			writer.println(getSerializedConfig());
		}
	}

	@Override
	public void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		if (request.getMethod().equalsIgnoreCase("PATCH")) {
			doPatch(request, response);
		} else {
			super.service(request, response);
		}
	}

	private String getSerializedConfig() throws JsonProcessingException {
		SerializedConfig sconfig = new SerializedConfig();
		String filterstring = this.context.getFilter().toString();
		if (!filterstring.equals("")) {
			sconfig.filter = filterstring;
		}
		return mapper.writeValueAsString(sconfig);
	}

	private void applySerializedConfig(SerializedConfig sconfig) throws InvalidFilterException {
		if (sconfig.filter != null) {
			context.getFilter().set(sconfig.filter);
			LOGGER.info("updated filter: " + sconfig.filter);
		}
	}
}

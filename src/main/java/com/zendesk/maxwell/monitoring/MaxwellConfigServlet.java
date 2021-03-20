package com.zendesk.maxwell.monitoring;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.filtering.InvalidFilterException;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.stream.Collectors;

/**
 * An HTTP servlet which allows for the live reconfiguration of maxwell.
 */
public class MaxwellConfigServlet extends HttpServlet {
  private static final ObjectMapper mapper = new ObjectMapper();
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
      return String.format("{\"error\":%s}",  mapper.writeValueAsString(errResponse));
    }
  }

  protected void doPatch(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    SerializedConfig sconfig;
    try {
      String reqbody = req.getReader().lines().collect(Collectors.joining(System.lineSeparator()));
      sconfig = mapper.readValue(reqbody, SerializedConfig.class);
    } catch(Exception e) {
      resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      try (PrintWriter writer = resp.getWriter()) {
        writer.println(ErrorResponse.getErrorResponseString(HttpServletResponse.SC_BAD_REQUEST, String.format("error processing body: %s", e.getMessage())));
      }
      return;
    }

    try {
      applySerializedConfig(sconfig);

      resp.setStatus(HttpServletResponse.SC_OK);
      try (PrintWriter writer = resp.getWriter()) {
        writer.println(getSerializedConfig());
      }
    } catch (InvalidFilterException e) {
      resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      try (PrintWriter writer = resp.getWriter()) {
        writer.println(ErrorResponse.getErrorResponseString(HttpServletResponse.SC_BAD_REQUEST, String.format("invalid filter: %s", e.getMessage())));
      }
    }
  }

  @Override protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    resp.setStatus(HttpServletResponse.SC_OK);
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
    sconfig.filter = this.context.getFilter().toString();
    return mapper.writeValueAsString(sconfig);
  }

  private void applySerializedConfig(SerializedConfig sconfig) throws InvalidFilterException {
    if (sconfig.filter != null) {
      context.getFilter().set(sconfig.filter);
    }
  }
}

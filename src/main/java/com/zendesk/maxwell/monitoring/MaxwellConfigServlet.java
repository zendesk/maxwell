package com.zendesk.maxwell.monitoring;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.filtering.InvalidFilterException;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.stream.Collectors;

/**
 * An HTTP servlets which allows for the live reconfiguration of maxwell.
 */
public class MaxwellConfigServlet extends HttpServlet {
  private static final long serialVersionUID = 3772654177231086757L;
  private static final ObjectMapper mapper = new ObjectMapper();
  private final MaxwellContext context;

  public MaxwellConfigServlet(MaxwellContext context) {
    super();
    this.context = context;
  }


  public static class ConfigRequest {

    private String filter;

    public String getFilter() {
      return this.filter;
    }
  }

  protected void doPatch(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    try {
      String reqbody = req.getReader().lines().collect(Collectors.joining(System.lineSeparator()));
      ConfigRequest creq = mapper.readValue(reqbody, ConfigRequest.class);
      this.context.getFilter().reset(creq.getFilter());

      resp.setStatus(HttpServletResponse.SC_OK);
    } catch (InvalidFilterException e) {
      resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      try (PrintWriter writer = resp.getWriter()) {
        writer.println("INVALID FILTER");
      }
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

}

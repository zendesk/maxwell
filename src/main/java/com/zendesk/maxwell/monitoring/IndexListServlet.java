package com.zendesk.maxwell.monitoring;


import org.apache.commons.lang3.tuple.Pair;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

public class IndexListServlet extends HttpServlet {
	private ArrayList<Pair<String, String>> endpoints = new ArrayList<>();
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		resp.setStatus(200);
		resp.setContentType("text/html");

		PrintWriter writer = resp.getWriter();
		writer.println("<head><title>Maxwell's Daemon</title></head><body>");
		writer.println("<h1>Maxwell's Daemon</h1>");
		writer.println("<ul>");

		for ( Pair<String, String> endpoint : endpoints ) {
			String li = String.format("\t<li><a href='%s'>%s</a> -- %s</li>", endpoint.getLeft(), endpoint.getLeft(), endpoint.getRight());
			writer.println(li);
		}
		writer.println("</ul>");
		writer.println("</body>");
	}

	public void addLink(String endpoint, String description) {
		endpoints.add(Pair.of(endpoint, description));

	}
}

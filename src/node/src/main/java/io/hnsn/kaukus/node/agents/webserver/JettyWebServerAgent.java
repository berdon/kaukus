package io.hnsn.kaukus.node.agents.webserver;

import io.hnsn.kaukus.configuration.NodeConfiguration;
import io.hnsn.kaukus.node.agents.AgentException;
import io.hnsn.kaukus.node.agents.quorum.QuorumAgent;
import io.hnsn.kaukus.node.agents.storage.StorageAgent;
import io.hnsn.kaukus.types.Namespace;
import io.hnsn.kaukus.utilities.Ref;
import java.io.IOException;
import java.util.stream.Collectors;
import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

@Slf4j
@RequiredArgsConstructor
public class JettyWebServerAgent implements WebServerAgent {
  private Server server;
  private final NodeConfiguration nodeConfiguration;
  private final QuorumAgent quorumAgent;
  private final StorageAgent storageAgent;

  @Override
  public void start() throws AgentException {
    try {
      server = new Server(nodeConfiguration.getWebServerPort());
      final var servletHandler = new ServletContextHandler(server, "/kaukus/");
      final var servletHolder = new ServletHolder(new Servlet(quorumAgent, storageAgent));
      servletHandler.addServlet(servletHolder, "/");
      server.start();
    } catch (Exception e) {
      throw new AgentException("Error starting the jetty server.", e);
    }
  }

  @SneakyThrows
  @Override
  public void close() throws IOException {
    try {
      server.stop();
    } catch (Exception e) {
      throw new AgentException("Error stopping the jetty server.", e);
    }
  }

  @RequiredArgsConstructor
  public static class Servlet extends HttpServlet {
    private final QuorumAgent quorumAgent;
    private final StorageAgent storageAgent;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      final var tokens = req.getServletPath().substring(1).split("/");
      if (tokens.length != 2) {
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        return;
      }

      final var namespaceString = tokens[0];
      final var key = tokens[1];

      final var namespace = new Ref<Namespace>();
      if (!Namespace.tryParse(namespaceString, namespace)) {
        log.error("Failed to parse request's namespace {}.", namespaceString);
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        return;
      }

      final var value = storageAgent.get(namespace.getValue(), key);
      if (value == null) {
        resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
      }
      else {
        resp.getWriter().write(value);
        resp.getWriter().flush();
        resp.getWriter().close();
        resp.setStatus(HttpServletResponse.SC_OK);
      }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      final var tokens = req.getServletPath().substring(1).split("/");
      if (tokens.length != 2) {
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        return;
      }

      final var namespaceString = tokens[0];
      final var key = tokens[1];
      final var value = req.getReader().lines().collect(Collectors.joining());

      final var namespace = new Ref<Namespace>();
      if (!Namespace.tryParse(namespaceString, namespace)) {
        log.error("Failed to parse request's namespace {}.", namespaceString);
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        return;
      }

      quorumAgent.requestStorageSet(namespace.getValue(), key, value);

      resp.setStatus(HttpServletResponse.SC_NO_CONTENT);
    }

    @Override
    protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
      final var tokens = req.getServletPath().substring(1).split("/");
      if (tokens.length != 2) {
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        return;
      }

      final var namespaceString = tokens[0];
      final var key = tokens[1];

      final var namespace = new Ref<Namespace>();
      if (!Namespace.tryParse(namespaceString, namespace)) {
        log.error("Failed to parse request's namespace {}.", namespaceString);
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        return;
      }

      quorumAgent.requestStorageDelete(namespace.getValue(), key);

      resp.setStatus(HttpServletResponse.SC_OK);
    }
  }
}

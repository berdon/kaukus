/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package io.hnsn.kaukus;

import io.hnsn.kaukus.types.Namespace;
import io.hnsn.kaukus.utilities.Ref;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.text.MessageFormat;
import java.util.concurrent.atomic.AtomicBoolean;

import com.beust.jcommander.JCommander;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;

import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.impl.SimpleLogger;

import io.hnsn.kaukus.configuration.NodeConfiguration;
import io.hnsn.kaukus.directory.NodeDirectory;
import io.hnsn.kaukus.guice.LoggerProvider;
import io.hnsn.kaukus.guiceModules.LaunchModule;
import io.hnsn.kaukus.guiceModules.NodeModule;
import io.hnsn.kaukus.node.Node;
import io.hnsn.kaukus.parameters.NodeParameters;
import io.hnsn.kaukus.parameters.ResetCommand;
import io.hnsn.kaukus.persistence.LSMTree;
import sun.misc.Signal;

public class App {
    private static Injector LAUNCH_INJECTOR;
    private final NodeConfiguration configuration;
    private final Logger log;

    public static void main(String[] args) throws IOException {
        var parameters = new NodeParameters();
        var resetCommand = new ResetCommand();
        var jc = JCommander.newBuilder()
            .addObject(parameters)
            .addCommand("reset", resetCommand)
            .build();
        
        jc.parse(args);

        if (parameters.isVerbose()) {
            System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "TRACE");
        }

        LAUNCH_INJECTOR = Guice.createInjector(new LaunchModule(parameters));
        var app = LAUNCH_INJECTOR.getInstance(App.class);

        if (parameters.isHelp()) {
            jc.usage();
            return;
        }

        final var command = jc.getParsedCommand() != null ? jc.getParsedCommand() : "start";
        switch (command) {
            case "reset":
                app.ResetNode();
                break;
            default:
                app.Start();
        }
    }

    @Inject
    App(NodeConfiguration configuration, LoggerProvider loggerProvider) {
        this.configuration = configuration;
        this.log = loggerProvider.get();
    }

    private void ResetNode() throws IOException {
        // TODO: Force/verify
        var parentSystemStorePath = configuration.getSystemStorePath().getParent();
        var systemStoreName = configuration.getSystemStorePath().getFileName().toString().split("\\.")[0];
        var pathMatcher = FileSystems.getDefault().getPathMatcher(MessageFormat.format("regex:{0}/{1}(\\.[0-9]*)?$", parentSystemStorePath, systemStoreName));
        try (final var directory = Files.newDirectoryStream(parentSystemStorePath, pathMatcher::matches)){
            for (var file : directory) {
                log.info("Deleting {}", file);
                Files.delete(file);
            }
        }
    }

    private void Start() throws IOException {
        java.util.logging.Logger.getLogger("").setLevel(Level.ALL);
        var bufferedReader = new BufferedReader(new InputStreamReader(System.in));
        var systemStoreLSMTree = LSMTree.openOrCreate(configuration.getSystemStorePath());

        log.info("Compacting system store...");
        systemStoreLSMTree.compact();

        var running = new AtomicBoolean(true);
        Signal.handle(new Signal("INT"), signal -> running.set(false));
        var nodeInjector = LAUNCH_INJECTOR.createChildInjector(new NodeModule(systemStoreLSMTree, (message, error) -> {
            running.set(false);
        }));

        try (var node = nodeInjector.getInstance(Node.class)) {
            var directory = nodeInjector.getInstance(NodeDirectory.class);
            node.start();
            while (running.get()) {
                if (!bufferedReader.ready()) {
                    Thread.yield();
                    continue;
                }

                var line = bufferedReader.readLine();
                if (line.compareToIgnoreCase("q") == 0 || line.compareToIgnoreCase("quit") == 0) break;
                else if (line.compareToIgnoreCase("d") == 0) {
                    for (var pair : directory.entrySet()) {
                        log.info("{}: {} @ {}:{}", pair.getKey(), pair.getValue().getStatus(), pair.getValue().getAddress(), pair.getValue().getPort());
                    }
                }
                else if (line.startsWith("get ")) {
                    final var tokens = line.split(" ");
                    final var namespace = new Ref<Namespace>();
                    if (!Namespace.tryParse(tokens[1], namespace)) {
                        log.error("Invalid namespace.");
                        continue;
                    }
                    log.info(node.getStorageAgent().get(namespace.getValue(), tokens[2]));
                }
                else if (line.startsWith("set ")) {
                    final var tokens = line.split(" ");
                    final var namespace = new Ref<Namespace>();
                    if (!Namespace.tryParse(tokens[1], namespace)) {
                        log.error("Invalid namespace.");
                        continue;
                    }
                    node.getStorageAgent().set(namespace.getValue(), tokens[2], tokens[3]);
                }
            }
        }

        bufferedReader.close();
    }
}

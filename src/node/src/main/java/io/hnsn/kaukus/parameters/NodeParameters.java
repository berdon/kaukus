package io.hnsn.kaukus.parameters;

import com.beust.jcommander.Parameter;

import lombok.Getter;

@Getter
public class NodeParameters {
    @Parameter(names = "--id")
    private String identifier;

    @Parameter(names = "--system-store-path")
    private String systemStorePath;

    @Parameter(names = "--verbose")
    private boolean verbose;

    @Parameter(names = "--help", help = true)
    private boolean help;

    @Parameter(names = "--system-port")
    private Integer systemPort;

    @Parameter(names = "--system-address")
    private String systemAddress;

    @Parameter(names = "--broadcast-address")
    private String broadcastAddress;

    @Parameter(names = "--broadcast-port")
    private Integer broadcastPort;
}

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
}

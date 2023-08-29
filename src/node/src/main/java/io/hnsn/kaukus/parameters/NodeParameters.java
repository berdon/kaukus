package io.hnsn.kaukus.parameters;

import com.beust.jcommander.Parameter;

import com.beust.jcommander.Parameters;
import lombok.Getter;

@Getter
public class NodeParameters {
    @Parameter(names = "--id", description = "Specify a node identifier.")
    private String identifier;

    @Parameter(names = "--system-store-path", description = "Specify the system storage path; defaults to /etc/kaukus/system.")
    private String systemStorePath;

    @Parameter(names = "--data-store-path", description = "Specify the data storage path; defaults to /etc/kaukus/data.")
    private String dataStorePath;

    @Parameter(names = "--verbose", description = "Enable verbose logging.")
    private boolean verbose;

    @Parameter(names = "--help", help = true)
    private boolean help;

    @Parameter(names = "--system-port", description = "Specify the system port; defaults to 21000.")
    private Integer systemPort;

    @Parameter(names = "--system-address", description = "Specify the address bound by the system node.")
    private String systemAddress;

    @Parameter(names = "--broadcast-address", description = "Specify the address used for broadcast messages.")
    private String broadcastAddress;

    @Parameter(names = "--broadcast-port", description = "Specify the broadcast port; defaults to 21012.")
    private Integer broadcastPort;

    @Parameter(names = "--webserver-hostname", description = "Specify the webserver hostname; defaults to localhost.")
    private String webserverHostname;

    @Parameter(names = "--webserver-port", description = "Specify the webserver port; defaults to 3000.")
    private Integer webserverPort;

    @Parameter(names = "--election-timeout", description = "Specify the election timeout in seconds; defaults to 3s.")
    private Integer electionTimeout;
}

package io.hnsn.kaukus.parameters;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import lombok.Getter;

@Getter
@Parameters(commandDescription = "Reset the Kaukus node")
public class ResetCommand {
    @Parameter(names = "-f")
    private boolean force;
}

package de.hpi.isg.mdms.cli;


import de.hpi.isg.mdms.cli.commands.Command;
import de.hpi.isg.mdms.cli.exceptions.CliException;
import de.hpi.isg.mdms.cli.variables.ContextObject;
import de.hpi.isg.mdms.cli.variables.ContextReference;
import de.hpi.isg.mdms.cli.variables.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * This class describes the environment of the MDMS CLI and contains variables etc.
 */
public class SessionContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(SessionContext.class);
    public static final String RETURN_VAR_NAME = "return";

    private Namespace globalNamespace = new Namespace();

    private final Map<String, Command> commands = new HashMap<>();

    private boolean isExitRequested = false;

    public ContextObject registerVariable(String name, ContextObject var) {
        return this.globalNamespace.register(name, var);
    }

    public ContextObject deleteVariable(ContextReference reference) throws CliException {
        return this.globalNamespace.delete(reference);
    }

    public Command registerCommand(String name, Command command) {
        final Command oldCommand = this.commands.put(name, command);
        if (oldCommand != null) {
            LOGGER.warn("{} replaced {} on alias {}.", command, oldCommand, name);
        }
        return oldCommand;
    }

    public void requestExit() {
        this.isExitRequested = true;
    }

    public boolean isExitRequested() {
        return isExitRequested;
    }

    public Command getCommand(String alias) {
        return this.commands.get(alias);
    }

    public Map<String, Command> getCommands() {
        return Collections.unmodifiableMap(this.commands);
    }

    public Namespace getGlobalNamespace() {
        return globalNamespace;
    }

    public ContextObject getVariable(String name) {
        return this.globalNamespace.get(name);
    }


    public void setReturnValue(ContextObject returnValue) {
        registerVariable(RETURN_VAR_NAME, returnValue);
    }

    public ContextObject getReturnValue() {
        return getVariable(RETURN_VAR_NAME);
    }

    /**
     * Revokes the request for exit.
     */
    public void revokeExitRequest() {
        this.isExitRequested = false;
    }

    public void setGlobalNamespace(Namespace globalNamespace) {
        this.globalNamespace = globalNamespace;
    }
}

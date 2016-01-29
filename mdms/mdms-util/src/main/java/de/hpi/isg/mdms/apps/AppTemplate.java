/***********************************************************************************************************************
 * Copyright (C) 2014 by Sebastian Kruse
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/
package de.hpi.isg.mdms.apps;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Date;

/**
 * A template for apps that supports command-line parsing, runtime measurement, logging, providing execution metadata
 * etc.
 *
 * @author Sebastian Kruse
 */
public abstract class AppTemplate<TParameters> {

    /**
     * Logger customized to the actually instantiated subclass.
     */
    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    /** Keeps track of the start time of the program. */
    protected long programStartTime;

    /** Keeps track of the end time of the preparation phase of the program. */
    protected long preparationEndTime;

    /** Keeps track of the end time of the program. */
    protected long programEndTime;

    /** Stores metadata about the execution of this job. */
    protected final AppExecutionMetadata executionMetadata;

    /**
     * Creates a new {@link AppTemplate} instance.
     * 
     * @param args
     *        are, e.g., command line parameters to configure this program.
     */
    public AppTemplate(final String... args) {
        getLogger().info("Initializing with parameters {}.", Arrays.toString(args));
        initialize(args);
        this.executionMetadata = new AppExecutionMetadata();
        this.executionMetadata.setParameters(args);
    }

    /**
     * This method is called on job initialization. Subclasses have to process the given arguments and configure this
     * instance.
     * 
     * @param args
     *        are the same parameters as in the constructor {@link #AppTemplate(String...)}.
     */
    protected abstract void initialize(String... args);
    
    /**
     * This utility method can be used to configure this job with JCommander. It requires to override
     * {@link #createParameters()}.
     * 
     * @param args
     *        have to be the parameters from {@link #initialize(String...)}.
     * @return the parsed JCommander parameter object
     */
    protected TParameters parseCommandLine(final String[] args) {
    
        try {
            final TParameters parameters = createParameters();
            new JCommander(parameters, args);
            return parameters;
        } catch (final ParameterException e) {
            System.err.println(e.getMessage());
            StringBuilder sb = new StringBuilder();
            new JCommander(createParameters()).usage(sb);
            for (String line : sb.toString().split("\n")) {
                System.err.println(line);
            }
            System.exit(1);
        }
    
        throw new RuntimeException("Execution flow should not pass this point.");
    }

    protected TParameters createParameters() {
        throw new UnsupportedOperationException(
                "Method must be implemented by subclass.");
    }

    /**
     * This method can either be used to obtain a default {@link Logger} for subclasses or can be overridden to provide
     * a {@link Logger} for the methods of {@link AbstractMetadataStoreJob}.
     * 
     * @return the logger to be used
     */
    protected Logger getLogger() {
        return this.logger;
    }

    /**
     * Starts the execution of this job.
     * 
     * @throws Exception
     *         if any error occurs during the execution
     */
    public void run() throws Exception {
        getLogger().info("Starting job execution.", new Date().toString(),
                this.getClass().getSimpleName());
        this.programStartTime = System.currentTimeMillis();

        
        // Execute program logic.
        try {
            prepareProgramLogic();
            this.preparationEndTime = System.currentTimeMillis();
            executeProgramLogic();
            this.executionMetadata.setIsProgramSuccess(true);
        } catch (final Exception e) {
            getLogger().error("Program execution failed.", e);
            e.printStackTrace();
            this.executionMetadata.setIsProgramSuccess(false);
        }

        this.programEndTime = System.currentTimeMillis();
        printProgramStatistics();

        if (isCleanUpRequested()) {
            getLogger().info("Cleaning up.");
            cleanUp();
            getLogger().info("Finished cleaning up.");
        }
        
        onExit();

    }

    /**
     * Subclasses can override/extend this method to prepare the program.
     * @throws Exception 
     */
    protected void prepareProgramLogic() throws Exception {
    }

    /**
     * This method contains the actual work of this program.
     * @throws Exception 
     */
    abstract protected  void executeProgramLogic() throws Exception;

    /**
     * This method is called after the program execution and logs program statistics.
     */
    protected void printProgramStatistics() {
        getLogger().info("Finished job exection.");
        getLogger().info("Runtime: {} ms", this.programEndTime - this.programStartTime);
    }

    /**
     * Subclasses need to overwrite this method to signal if a clean up is desired after job execution.
     */
    abstract protected boolean isCleanUpRequested();

    /**
     * Subclasses can overwrite this method to perform clean up work if this is requested by {@link #cleanUp()}.
     */
    protected void cleanUp() throws Exception {
        // empty method implementation
    }
    
    /**
     * Subclasses can override this method to perform work after the program has finished. 
     */
    protected void onExit() {
        // nothing
    }

    public AppExecutionMetadata getExecutionMetadata() {
        return this.executionMetadata;
    }
    
}

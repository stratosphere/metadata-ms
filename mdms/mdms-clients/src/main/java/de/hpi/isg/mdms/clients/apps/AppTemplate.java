/***********************************************************************************************************************
 * Copyright (C) 2014 by Sebastian Kruse
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/
package de.hpi.isg.mdms.clients.apps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    /** Keeps track of the start time of the program. */
    protected long appStartTime;

    /** Keeps track of the end time of the preparation phase of the program. */
    protected long preparationEndTime;

    /** Keeps track of the end time of the program. */
    protected long appEndTime;

    /** Stores metadata about the execution of this app. */
    protected final AppExecutionMetadata executionMetadata;

    /** Stores the parameters for this app execution. */
    protected final TParameters parameters;

    /**
     * Creates a new {@link AppTemplate} instance.
     *
     * @param parameters
     *        are, e.g., command line parameters to configure this app.
     */
    public AppTemplate(TParameters parameters) {
        this.parameters = parameters;
        this.executionMetadata = new AppExecutionMetadata();
        this.executionMetadata.setParameters(parameters);
    }

    /**
     * This method can either be used to obtain a default {@link Logger}.
     *
     * @return the logger to be used
     */
    protected Logger getLogger() {
        return this.logger;
    }

    /**
     * Starts the execution of this app.
     *
     * @throws Exception
     *         if any error occurs during the execution
     */
    public void run() throws Exception {
        getLogger().info("Starting app execution.", new Date().toString(),
                this.getClass().getSimpleName());
        this.appStartTime = System.currentTimeMillis();


        // Execute app logic.
        try {
            prepareAppLogic();
            this.preparationEndTime = System.currentTimeMillis();
            executeAppLogic();
            this.executionMetadata.setAppSuccess(true);
        } catch (final Exception e) {
            getLogger().error("Program execution failed.", e);
            e.printStackTrace();
            this.executionMetadata.setAppSuccess(false);
        }

        this.appEndTime = System.currentTimeMillis();
        printProgramStatistics();

        if (isCleanUpRequested()) {
            getLogger().info("Cleaning up.");
            cleanUp();
            getLogger().info("Finished cleaning up.");
        }

        onExit();

    }

    /**
     * Subclasses can override/extend this method to prepare the app.
     * @throws Exception
     */
    protected void prepareAppLogic() throws Exception {
    }

    /**
     * This method contains the actual work of this app.
     * @throws Exception
     */
    abstract protected void executeAppLogic() throws Exception;

    /**
     * This method is called after the app execution and logs app statistics.
     */
    protected void printProgramStatistics() {
        getLogger().info("Finished app exection.");
        getLogger().info("Runtime: {} ms", this.appEndTime - this.appStartTime);
    }

    /**
     * Subclasses need to overwrite this method to signal if a clean up is desired after app execution.
     */
    abstract protected boolean isCleanUpRequested();

    /**
     * Subclasses can overwrite this method to perform clean up work if this is requested by {@link #cleanUp()}.
     */
    protected void cleanUp() throws Exception {
        // empty method implementation
    }

    /**
     * Subclasses can override this method to perform work after the app has finished.
     */
    protected void onExit() {
        // nothing
    }

    public AppExecutionMetadata getExecutionMetadata() {
        return this.executionMetadata;
    }

}

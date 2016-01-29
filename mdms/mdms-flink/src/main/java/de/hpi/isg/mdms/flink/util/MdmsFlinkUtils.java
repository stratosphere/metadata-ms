package de.hpi.isg.mdms.flink.util;

import de.hpi.isg.mdms.model.location.Location;
import de.hpi.isg.mdms.model.targets.Target;
import org.apache.flink.core.fs.Path;

/**
 * Utilities for the interaction of MDMS and Flink.
 */
public class MdmsFlinkUtils {

    /**
     * Returns a {@link Path} describing the location of the given {@link Target}.
     *
     * @param target for which the location is sought
     * @return the location of the table
     */
    public static Path getPath(final Target target) {
        Location location = target.getLocation();
        String path = location.get(Location.PATH);
        if (path == null) {
            throw new IllegalArgumentException(
                    String.format("%s does not seem to have a path. Available: %s",
                            target,
                            target.getLocation().getProperties().keySet()));
        }
        return new Path(path);
    }

}

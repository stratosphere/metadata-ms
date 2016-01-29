package de.hpi.isg.mdms.flink.util;


import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This class gathers a set of static methods that support activities related to file management.
 *
 * @author Sebastian Kruse
 */
public class FileUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileUtils.class);

    /**
     * Private constructor to avoid instantiation of this class.
     */
    private FileUtils() {
    }

    /**
     * Ensures that the given path points to an empty directory.
     *
     * @param parentPath is the location in which the empty directory is to be enforced
     * @param name       is the name of the empty directory to be enforced
     * @param fs         is the {@link FileSystem} for the path or <tt>null</tt>
     * @return the path of the empty directory
     * @throws IOException
     */
    public static Path ensureEmptyDirectory(final Path parentPath,
                                            final String name, final FileSystem fs) throws IOException {
        final Path targetPath = new Path(parentPath, name);
        ensureEmptyDirectory(targetPath, fs);
        return targetPath;
    }

    /**
     * Ensures that the given path points to an empty directory.
     *
     * @param path is the location where the empty directory is to be enforced
     * @param fs   is the {@link FileSystem} for the path or <tt>null</tt>
     * @throws IOException
     */
    public static void ensureEmptyDirectory(final Path path, FileSystem fs)
            throws IOException {

        fs = ensureFileSystem(path, fs);

        // Delete file/dir+children if existent.
        if (fs.exists(path)) {
            fs.delete(path, true);
        }

        // Recreate the path.
        fs.mkdirs(path);
    }

    /**
     * Retrieves the {@link FileSystem} for the path if it is not already given.
     *
     * @param path for which the {@link FileSystem} is needed
     * @param fs   can be already given or <tt>null</tt>
     * @return the given {@link FileSystem} or the newly retrieved one
     * @throws IOException
     */
    public static FileSystem ensureFileSystem(final Path path,
                                              final FileSystem fs) throws IOException {
        if (fs == null) {
            return path.getFileSystem();
        }

        return fs;
    }

    /**
     * Removes the given file or directory.
     *
     * @param file      is the file or directory to remove
     * @param recursive tells whether the recursive removal shall be applied to directories
     * @return whether the deletion was successful
     * @throws IOException
     */
    public static boolean remove(final Path file, final boolean recursive) throws IOException {
        final FileSystem fs = file.getFileSystem();
        return fs.delete(file, recursive);
    }
}

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
package de.hpi.isg.mdms.flink.readwrite;

import de.hpi.isg.mdms.Encoding;
import de.hpi.isg.mdms.flink.util.FileUtils;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntMaps;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This file input format reads files where each line contains a {@link String}. Multiple files can be used and each
 * read value is annotated with a source value.
 *
 * @author Sebastian Kruse
 */
public class MultiFileTextInputFormat extends DelimitedInputFormat<Tuple2<Integer, String>>
        implements ResultTypeQueryable<Tuple2<Integer, String>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiFileTextInputFormat.class);

    /**
     * Creates a {@link MultiFileTextInputFormat} only for the given input path. The file ID for this path is 0.
     *
     * @param inputPath is the path to process by this input format
     * @return the configured {@link MultiFileTextInputFormat} instance
     */
    public static MultiFileTextInputFormat createForSingleFile(String inputPath) {
        Object2IntMap<String> pathIds = Object2IntMaps.<String>singleton(inputPath, 0);
        final ListBasedFileIdRetriever fileIdRetriever = new MultiFileTextInputFormat.ListBasedFileIdRetriever(pathIds);
        MultiFileTextInputFormat inputFormat;
        inputFormat = new MultiFileTextInputFormat(fileIdRetriever, fileIdRetriever);
        inputFormat.setFilePath(inputPath);
        return inputFormat;
    }

    /**
     * Creates a {@link MultiFileTextInputFormat} only for the given input paths. Each file is assigned a unique file
     * ID in ascending order starting from 0.
     *
     * @param inputPaths are the paths to process by this input format
     * @return the configured {@link MultiFileTextInputFormat} instance
     */
    public static MultiFileTextInputFormat createFor(String... inputPaths) {
        if (inputPaths.length == 0) {
            throw new IllegalArgumentException("Did not provide any input paths.");
        }
        Object2IntMap<String> pathIds = new Object2IntOpenHashMap<>(inputPaths.length);
        int pathId = 0;
        for (String inputPath : inputPaths) {
            Path path = new Path(inputPath);
            try {
                if (!path.getFileSystem().exists(path)) {
                    throw new IllegalArgumentException("No such file found for " + path);
                }
            } catch (IOException e) {
                throw new RuntimeException("Could not verify existence of path " + path, e);
            }
            pathIds.put(inputPath, pathId++);
        }
        final ListBasedFileIdRetriever fileIdRetriever = new MultiFileTextInputFormat.ListBasedFileIdRetriever(pathIds);
        MultiFileTextInputFormat inputFormat;
        inputFormat = new MultiFileTextInputFormat(fileIdRetriever, fileIdRetriever);
        String inputPath = FileUtils.findCommonParent(Arrays.asList(inputPaths));
        inputFormat.setFilePath(inputPath);
        LOGGER.info("Using {} as parent input path.", inputPath);
        return inputFormat;
    }

    private static final long serialVersionUID = 1L;

    private static final Charset UTF_8 = Charset.forName("UTF-8");

    private Integer fileId;

    private Encoding encoding;

    /**
     * Can be used to filter the files from an input directory.
     */
    private FileFilter fileFilter = NullFileFilter.INSTANCE;

    /**
     * Used to map a {@link FileInputSplit} to an ID.
     */
    private final FileIdRetriever fileIdRetriever;

    /** Used to map an {@link Encoding} to each file. */
    private final Map<String, Encoding> fileEncodings;

    public MultiFileTextInputFormat(final FileFilter fileFilter, final FileIdRetriever fileIdRetriever) {
        this(fileFilter, fileIdRetriever, null);
    }

    public MultiFileTextInputFormat(final FileFilter fileFilter, final FileIdRetriever fileIdRetriever,
                                    Map<String, Encoding> fileEncodings) {
        super();
        this.fileFilter = fileFilter;
        this.fileIdRetriever = fileIdRetriever;
        this.fileEncodings = fileEncodings;
    }

    @Override
    public void open(final FileInputSplit split) throws IOException {
        LOGGER.trace("Opening {}.", split);

        this.encoding = retrieveEncoding(split.getPath());
        super.open(split);

        // If this instance is configured to read a certain FileInputSplit,
        // we remember the ID of this split to output it with the read values.
        this.fileId = this.fileIdRetriever.retrieve(split);
    }

    /**
     * Instead of specifying an encoding per file, this setter allows to set the same encoding for all files.
     */
    public void setEncoding(Encoding encoding) {
        this.encoding = encoding;
    }


    /**
     * Find the specified encoding for the given file. If none is specified, the default encoding is returned instead.
     * If no encoding mapping was given at all, the preset encoding is used.
     */
    private Encoding retrieveEncoding(Path file) {
        if (this.fileEncodings != null) {
            Encoding encoding = this.fileEncodings.get(file.toString());
            if (encoding != null) {
                return encoding;
            }
        }
        return this.encoding == null ? Encoding.DEFAULT_ENCODING : this.encoding;
    }

    @Override
    public Tuple2<Integer, String> readRecord(final Tuple2<Integer, String> reuse, final byte[] bytes,
                                              final int offset, final int numBytes) {

        reuse.f0 = this.fileId;
        reuse.f1 = new String(bytes, offset, numBytes, this.encoding.getCharset());

        return reuse;
    }

    @Override
    protected boolean acceptFile(final FileStatus fileStatus) {
        // If the file of this format is a directory (which this format expects), we need to make sure
        // that this base directory passes the file filter.
        boolean isBaseDir = false;
        try {
            // There seem to be problems in the path comparison, so we need to hack a little bit.
            final Path path = getFilePath();
            final FileSystem fileSystem = path.getFileSystem();
            isBaseDir = fileSystem.getFileStatus(path).getPath().equals(fileStatus.getPath());
        } catch (final IOException e) {
            e.printStackTrace();
        }
        return isBaseDir || this.fileFilter.accept(fileStatus);
    }

    @Override
    protected boolean testForUnsplittable(FileStatus pathFile) {
        boolean isFileUnsplittable = super.testForUnsplittable(pathFile) ||
                FileUtils.findInflaterInputStreamFactory(pathFile.getPath()) != null;
        this.unsplittable |= isFileUnsplittable;
        return isFileUnsplittable;
    }

    @Override
    public TypeInformation<Tuple2<Integer, String>> getProducedType() {
        return TupleTypeInfo.getBasicTupleTypeInfo(Integer.class, String.class);
    }

    /**
     * Sets the file filter to be used.
     *
     * @param fileFilter is the {@link FileFilter} instance to be used for the filtering
     */
    public void setFileFilter(final FileFilter fileFilter) {
        this.fileFilter = fileFilter;
    }

    /**
     * Creates a new {@link FileFilter} that filters using a regex.
     *
     * @param regex is the regex that should be used for the file filtering
     * @return the {@link RegexFileFilter}
     */
    public static RegexFileFilter createRegexFileFilter(final String regex) {
        return new RegexFileFilter(regex);
    }

    /**
     * This interface describes a filter for files.
     *
     * @author Sebastian Kruse
     */
    public static interface FileFilter extends Serializable {

        /**
         * Tells whether the file shall be accepted, i.e. not filtered out.
         *
         * @param fileStatus describes the file of interest
         * @return whether the file shall pass the filter
         */
        boolean accept(FileStatus fileStatus);

    }

    public static class NullFileFilter implements FileFilter {

        private static final long serialVersionUID = 1L;

        public static final NullFileFilter INSTANCE = new NullFileFilter();

        @Override
        public boolean accept(final FileStatus fileStatus) {
            return true;
        }

    }

    /**
     * This {@link FileFilter} implementation filter files based on a given regex.
     *
     * @author Sebastian Kruse
     */
    private static class RegexFileFilter implements FileFilter {

        private static final long serialVersionUID = 1L;

        private final Pattern pattern;

        public RegexFileFilter(final String regex) {
            super();
            this.pattern = Pattern.compile(regex);
        }

        @Override
        public boolean accept(final FileStatus fileStatus) {
            final String fileName = fileStatus.getPath().getName();
            final Matcher matcher = this.pattern.matcher(fileName);
            return matcher.matches();
        }

    }

    /**
     * A {@link FileIdRetriever} maps {@link FileInputSplit} objects to IDs.
     *
     * @author Sebastian Kruse
     */
    public static interface FileIdRetriever extends Serializable {

        /**
         * Retrieves the ID for the given file input split.
         *
         * @param fileInputSplit is the one for which the ID shall be retrieved
         * @return the ID
         */
        int retrieve(FileInputSplit fileInputSplit);

    }

    public static class ListBasedFileIdRetriever implements FileIdRetriever, FileFilter {

        private static final long serialVersionUID = 1L;

        private final Object2IntMap<String> path2Id;

        public ListBasedFileIdRetriever(final Object2IntMap<String> path2Id) {
            super();
            this.path2Id = path2Id;
        }

        @Override
        public int retrieve(final FileInputSplit fileInputSplit) {
            final String path = fileInputSplit.getPath().toString();
            return this.path2Id.getInt(path);
        }

        @Override
        public boolean accept(final FileStatus fileStatus) {
            return this.path2Id.containsKey(fileStatus.getPath().toString());
        }

    }

    /**
     * This class uses the hash code of the file paths as IDs for the files.
     */
    public static class HashBasedFileIdRetriever implements FileIdRetriever {

        public static final HashBasedFileIdRetriever INSTANCE = new HashBasedFileIdRetriever();

        private HashBasedFileIdRetriever() {
        }

        @Override
        public int retrieve(FileInputSplit fileInputSplit) {
            return fileInputSplit.getPath().hashCode();
        }
    }

}

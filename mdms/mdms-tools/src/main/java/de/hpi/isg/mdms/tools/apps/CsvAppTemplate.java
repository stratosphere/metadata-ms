package de.hpi.isg.mdms.tools.apps;


import de.hpi.isg.mdms.Encoding;
import de.hpi.isg.mdms.clients.parameters.CsvParameters;
import de.hpi.isg.mdms.flink.apps.FlinkAppTemplate;
import de.hpi.isg.mdms.flink.util.FileUtils;
import de.hpi.isg.mdms.tools.util.AttributeIndexer;
import de.hpi.isg.mdms.tools.util.CsvUtils;
import de.hpi.isg.mdms.util.IoUtils;
import de.hpi.isg.mdms.util.Reference;
import org.apache.flink.core.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class gives a template for jobs that profile with CSV files. In particular, it takes care of resolving input
 * directories to files and indexing the columns in all files with unique IDs.
 *
 * @author Sebastian Kruse
 */
public abstract class CsvAppTemplate<TParameters> extends FlinkAppTemplate<TParameters> {

    /**
     * Is responsible for giving unique IDs to the columns of all CSV files.
     */
    protected AttributeIndexer attributeIndexer;

    /**
     * Captures the encodings of files that deviate from the default encoding.
     *
     * @see Encoding#DEFAULT_ENCODING
     */
    protected Map<String, Encoding> fileEncodings;


    private List<Path> inputFiles;

    /**
     * Creates a new instance
     *
     * @see FlinkAppTemplate#FlinkAppTemplate(Object)
     */
    public CsvAppTemplate(final TParameters parameters) {
        super(parameters);
    }

    @Override
    protected void prepareAppLogic() throws Exception {
        super.prepareAppLogic();

        this.inputFiles = FileUtils.gatherFiles(getFileFetchDepth(), getInputFiles().toArray(new String[0]));
        inspectFiles(this.inputFiles);
    }

    @Override
    public void executeAppLogic() throws Exception {
        // For backwards compatibility, give old jobs the input files directly.
        executeProgramLogic(this.inputFiles);
    }

    /**
     * Indexes the attributes of the files and detects the encodings of the files.
     *
     * @param files are the files to inspectFiles
     * @throws IOException
     */
    private void inspectFiles(final List<Path> files) throws IOException {

        this.attributeIndexer = new AttributeIndexer();
        this.fileEncodings = new HashMap<>();
        final char fieldSeparatorChar = getCsvParameters()
                .getFieldSeparatorChar();

        Reference<InputStream> gnawedInputStreamReference = new Reference<>();
        for (final Path file : files) {
            try {
                Encoding encoding = FileUtils.detectEncoding(file, null, Encoding.DEFAULT_ENCODING,
                        gnawedInputStreamReference);
                getLogger().debug("Detected encoding {} for {}.", encoding, file);
                if (!encoding.equals(Encoding.DEFAULT_ENCODING)) {
                    this.fileEncodings.put(file.toString(), encoding);
                }
                BufferedReader reader = new BufferedReader(encoding.applyTo(gnawedInputStreamReference.get()));
                final int numColumns = CsvUtils.countAttributes(reader.readLine(),
                        (char) fieldSeparatorChar, getCsvParameters().getQuoteChar());
                this.attributeIndexer.index(file.getName(), numColumns);
                IoUtils.closeSafe(gnawedInputStreamReference.get());

            } catch (IOException e) {
                // Intercept for a more detailed message.
                getLogger().error("Error when processing {}.", file);
                throw e;
            } finally {
                IoUtils.close(gnawedInputStreamReference.get());
            }
        }

        getLogger().trace(this.attributeIndexer.toTuples().toString());
    }

//    /**
//     * Creates a {@link DataSource} for the given file if it is not empty.
//     *
//     * @param file is the CSV file for which the {@link DataSource} shall be created
//     * @return the {@link DataSource} or <tt>null</tt> if the file is empty
//     * @throws IOException
//     * @deprecated rather use {@link #createCsvDataSource(Path, int)}
//     */
//    @Deprecated
//    protected DataSource<Record> createCsvDataSource(final Path file)
//            throws IOException {
//
//        return createCsvDataSource(file, -1);
//    }

//    /**
//     * Creates a {@link DataSource} for the given file if it is not empty.
//     *
//     * @param file is the CSV file for which the {@link DataSource} shall be created
//     * @return the {@link DataSource} or <tt>null</tt> if the file is empty
//     * @throws IOException
//     */
//    protected DataSource<Record> createCsvDataSource(final Path file, final int degreeOfParallelism)
//            throws IOException {
//
//        final int numColumns = this.attributeIndexer.getNumAttributes(file
//                .getName());
//        if (numColumns == 0) {
//            return null;
//        }
//
//        final char fieldSeparator = getCsvParameters().getFieldSeparatorChar();
//        final DataSource<Record> dataSource = CsvUtils
//                .createRecordDataSource(file, numColumns, fieldSeparator, this.executionEnvironment);
//        return dataSource;
//    }

//    /**
//     * Proposes degree of parallelism settings for a set of file sinks by assigning larger DOPs to larger files.
//     *
//     * @param files      are the files that shall be read
//     * @param fs         is the optional {@link FileSystem} of the files
//     * @return a map that assigns each given file a proposed degree of parallelism
//     * @throws IOException
//     */
//    protected Map<Path, Integer> estimateGoodDegreesOfParallelism(final Collection<Path> files, FileSystem fs)
//            throws IOException {
//
//        if (fs == null) {
//            fs = CollectionUtils.getAny(files).getFileSystem();
//        }
//
//        final int overallDop = getMaxDop();
//
//        long sumFileSizes = 0;
//        final Collection<FileStatus> fileStatuses = new ArrayList<FileStatus>();
//        for (final Path file : files) {
//            final FileStatus fileStatus = fs.getFileStatus(file);
//            fileStatuses.add(fileStatus);
//            sumFileSizes += fileStatus.getLen();
//        }
//
//        final Map<Path, Integer> dopEstimates = new HashMap<Path, Integer>();
//        for (final FileStatus fileStatus : fileStatuses) {
//            final double fileSizeShare = fileStatus.getLen() / (double) sumFileSizes;
//            final int dop = Math.max(1, (int) (Math.round(overallDop * fileSizeShare)));
//            System.out.format("[DOP] %s has a size share of %.2f%% and gets a DOP of %d/%d.\n", fileStatus.getPath(),
//                    fileSizeShare * 100, dop, overallDop);
//            dopEstimates.put(fileStatus.getPath(), Math.max(dop, 1));
//        }
//
//        return dopEstimates;
//    }

//    public boolean isDopSet() {
//        return getFlinkParameters().degreeOfParallelism > 0;
//    }

//    /**
//     * @return the degree of parallelism specified by the user or {@link #DEFAULT_DOP}.
//     */
//    public int getMaxDop() {
//        int overallDop = getFlinkParameters().degreeOfParallelism;
//        if (overallDop < 0) {
//            overallDop = DEFAULT_DOP;
//        }
//        return overallDop;
//    }

    /**
     * Subclasses must implement this method and execute their specific logic here. In particular, they can use the
     * ready-made {@link #executionEnvironment} to run Flink jobs and use the {@link #attributeIndexer} to refer
     * to CSV columns via IDs
     *
     * @param files
     * @throws Exception if the job execution fails
     */
    abstract protected void executeProgramLogic(List<Path> files) throws Exception;

    /**
     * Subclasses must provide {@link CsvParameters} for this kind of app, typically drawn from the {@link #parameters}.
     *
     * @return the {@link CsvParameters}
     */
    abstract protected CsvParameters getCsvParameters();

    @Override
    protected void cleanUp() throws Exception {
        // implementation for backwards compatibility
        super.cleanUp();
        cleanUp(this.inputFiles);
    }

    /**
     * This method is called if cleaning operations are requested after the job execution, in particular deleting
     * temporary files.
     *
     * @param files are the input files of the job (same as in {@link #executeProgramLogic(List)})
     * @throws Exception if the cleansing fails
     */
    protected void cleanUp(final List<Path> files) throws Exception {
        if (this.tempFolder != null) {
            if (!FileUtils.remove(this.tempFolder, true)) {
                System.err.format("Could not remove temporary folder %s.", this.tempFolder);
            }
        }
    }

    /**
     * Subclasses must implement this method and return the input file paths (CSV files or directories).
     *
     * @return the paths
     */
    abstract protected Collection<String> getInputFiles();

    /**
     * Returns the recursion depth when resolving input directories.
     */
    protected int getFileFetchDepth() {
        return 1;
    }

    /**
     * Creates a temporary folder located near one of the given files. This folder will be automatically deleted, unless
     * no clean-up is desired.
     *
     * @param files are a bunch of files that give a hint where to create the temp folder
     * @throws IOException
     */
    protected void prepareTempFolder(final List<Path> files) throws IOException {

        // Prepare temp folder.
        final Path arbitraryFile = files.get(0);
        final Path parent = arbitraryFile.getParent();
        this.tempFolder = FileUtils.ensureEmptyDirectory(parent, "temp", null);
    }

}
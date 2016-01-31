package de.hpi.isg.mdms.tools.metanome;

import de.hpi.isg.mdms.clients.parameters.MetadataStoreParameters;
import de.hpi.isg.mdms.model.MetadataStore;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.results.*;
import de.metanome.backend.result_receiver.ResultReceiver;
import org.apache.commons.lang.NotImplementedException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Writes all received results to a specified metadata store.
 * Receives the metanome results and extracts the relevant information.
 * These are passed to the {@link ResultWriter} to write them to the metadatastore.
 *
 * @author Susanne Buelow
 */
public class ResultMetadataStoreWriter extends ResultReceiver {

    private ResultWriter writer;

    public ResultMetadataStoreWriter(String algorithmExecutionIdentifier,
                                     MetadataStoreParameters metadatastoreParameters,
                                     String schemaName,
                                     String tableName,
                                     String resultDescription)
            throws FileNotFoundException {
        super(algorithmExecutionIdentifier);
        writer = new ResultWriter(metadatastoreParameters, schemaName, tableName, resultDescription);
    }

    public ResultMetadataStoreWriter(String algorithmExecutionIdentifier, MetadataStore metadatastore, String schemaName, String tableName,
                                     String resultDescription) throws FileNotFoundException {
        super(algorithmExecutionIdentifier);
        writer = new ResultWriter(metadatastore, schemaName, tableName, resultDescription);

    }


    /**
     * Method not implemented yet!
     * <p>
     * To implement:
     * create basicStatistic-representation in mdms;
     * extract table and column names from metanome-statistic to get column id in metadatastore;
     * create a method in {@link ResultWriter} to write statistic to metadatastore.
     */
    @Override
    public void receiveResult(BasicStatistic statistic) {
        throw new NotImplementedException();
    }

    @Override
    public void receiveResult(FunctionalDependency functionalDependency) {
        String rhsTableName = convertMetanomeTableIdentifier(functionalDependency.getDependant().getTableIdentifier());
        String rhsColumnName = convertMetanomeColumnIdentifier(functionalDependency.getDependant().getColumnIdentifier());

        Set<ColumnIdentifier> cc = functionalDependency.getDeterminant().getColumnIdentifiers();
        String[] lhsTableNames = new String[cc.size()];
        String[] lhsColumnNames = new String[cc.size()];
        int i = 0;
        for (ColumnIdentifier c : cc) {
            lhsTableNames[i] = convertMetanomeTableIdentifier(c.getTableIdentifier());
            lhsColumnNames[i] = convertMetanomeColumnIdentifier(c.getColumnIdentifier());
            i++;
        }
        writer.writeFD(rhsTableName, rhsColumnName, lhsTableNames, lhsColumnNames);
    }

    @Override
    public void receiveResult(InclusionDependency inclusionDependency) {
        List<ColumnIdentifier> DepColumnIdentifiers = inclusionDependency.getDependant().getColumnIdentifiers();
        List<ColumnIdentifier> RefColumnIdentifiers = inclusionDependency.getReferenced().getColumnIdentifiers();

        String[] depTableNames = new String[DepColumnIdentifiers.size()];
        String[] depColumnNames = new String[DepColumnIdentifiers.size()];
        String[] refTableNames = new String[RefColumnIdentifiers.size()];
        String[] refColumnNames = new String[RefColumnIdentifiers.size()];
        ColumnIdentifier dep;
        ColumnIdentifier ref;
        for (int i = 0; i < DepColumnIdentifiers.size(); i++) {
            dep = DepColumnIdentifiers.get(i);
            ref = RefColumnIdentifiers.get(i);
            depTableNames[i] = convertMetanomeTableIdentifier(dep.getTableIdentifier());
            depColumnNames[i] = convertMetanomeColumnIdentifier(dep.getColumnIdentifier());
            refTableNames[i] = convertMetanomeTableIdentifier(ref.getTableIdentifier());
            refColumnNames[i] = convertMetanomeColumnIdentifier(ref.getColumnIdentifier());
        }

        writer.writeIND(refTableNames, refColumnNames, depTableNames, depColumnNames);
    }

    @Override
    public void receiveResult(UniqueColumnCombination uniqueColumnCombination) {
        Set<ColumnIdentifier> cc = uniqueColumnCombination.getColumnCombination().getColumnIdentifiers();
        String[] uccTableNames = new String[cc.size()];
        String[] uccColumnNames = new String[cc.size()];
        int i = 0;
        for (ColumnIdentifier c : cc) {
            uccTableNames[i] = convertMetanomeTableIdentifier(c.getTableIdentifier());
            uccColumnNames[i] = convertMetanomeColumnIdentifier(c.getColumnIdentifier());
            i++;
        }
        writer.writeUCC(uccTableNames, uccColumnNames);
    }

    /**
     * Method not implemented yet!
     * <p>
     * To implement:
     * create cUcc-representation in mdms;
     * extract table and column names from metanome-cUcc to get column id in metadatastore;
     * create a method in {@link ResultWriter} to write cUcc to metadatastore.
     */
    public void receiveResult(ConditionalUniqueColumnCombination conditionalUniqueColumnCombination) {
        throw new NotImplementedException();
    }

    /**
     * Method not implemented yet!
     * <p>
     * To implement:
     * create od-representation in mdms;
     * extract table and column names from metanome-od to get column id in metadatastore;
     * create a method in {@link ResultWriter} to write od to metadatastore.
     */
    @Override
    public void receiveResult(OrderDependency orderDependency) throws CouldNotReceiveResultException {
        throw new NotImplementedException();
    }

    public static String convertMetanomeTableIdentifier(String tableIdentifier) {
        if (tableIdentifier != null && !tableIdentifier.isEmpty()) {
            return tableIdentifier.substring(tableIdentifier.lastIndexOf(File.separator) + 1);
        } else {
            return "";
        }
    }

    public static String convertMetanomeColumnIdentifier(String columnIdentifier) {
        if (columnIdentifier.contains("column")) {
            try {
                return "[" + (Integer.valueOf(columnIdentifier.replace("column", "")) - 1) + "]";
            } catch (NumberFormatException e) {
            }
        }
        return columnIdentifier;
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }

}

package de.hpi.isg.mdms.tools.apps;

import de.hpi.isg.mdms.domain.constraints.FunctionalDependency;
import de.hpi.isg.mdms.domain.constraints.InclusionDependency;
import de.hpi.isg.mdms.domain.constraints.UniqueColumnCombination;
import de.hpi.isg.mdms.model.DefaultMetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.location.DefaultLocation;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.mdms.model.util.ReferenceUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Test suite for the {@link MetanomeStatisticsImportApp}.
 */
public class MetanomeDependencyImportAppTest {

    @Test
    public void testImportFdsFromJson() throws Exception {
        DefaultMetadataStore metadataStore = new DefaultMetadataStore();
        Schema schema = metadataStore.addSchema("WDC", "", new DefaultLocation());
        Table table = schema.addTable(metadataStore, "WDC_astronomical.csv", "", new DefaultLocation());
        Column name = table.addColumn(metadataStore, "Name", "", 0);
        Column iau = table.addColumn(metadataStore, "IAU abbreviation", "", 1);
        Column symbol = table.addColumn(metadataStore, "Symbol", "", 2);
        Column unicodeCodepoint = table.addColumn(metadataStore, "Unicode codepoint", "", 3);
        Column unicodeDisplay = table.addColumn(metadataStore, "Unicode display", "", 4);
        Column represents = table.addColumn(metadataStore, "Represents", "", 5);

        File inputFile = new File(Thread.currentThread().getContextClassLoader().getResource("metanome-fds.json").toURI());

        MetanomeDependencyImportApp.fromParameters(
                metadataStore,
                inputFile.getAbsolutePath(),
                ".+",
                "json",
                "FD",
                "WDC",
                "WDC",
                null
        );

        Collection<ConstraintCollection<FunctionalDependency>> constraintCollections =
                metadataStore.getConstraintCollectionByConstraintTypeAndScope(
                        FunctionalDependency.class, metadataStore.getTargetByName("WDC.WDC_astronomical.csv")
                );
        Assert.assertEquals(1, constraintCollections.size());
        ConstraintCollection<FunctionalDependency> constraintCollection = constraintCollections.iterator().next();
        Assert.assertEquals(4, constraintCollection.getConstraints().size());

        Set<FunctionalDependency> discoveredFds = new HashSet<>(constraintCollection.getConstraints());
        Set<FunctionalDependency> expectedFds = new HashSet<>();
        expectedFds.add(new FunctionalDependency(new int[0], symbol.getId()));
        expectedFds.add(new FunctionalDependency(new int[]{iau.getId()}, name.getId()));
        expectedFds.add(new FunctionalDependency(new int[]{name.getId()}, iau.getId()));
        expectedFds.add(new FunctionalDependency(new int[]{unicodeCodepoint.getId(), represents.getId()}, iau.getId()));

        Assert.assertEquals(expectedFds, discoveredFds);
    }

    @Test
    public void testImportIndsFromJson() throws Exception {
        DefaultMetadataStore metadataStore = new DefaultMetadataStore();
        Schema schema = metadataStore.addSchema("SG", "", new DefaultLocation());
        Table sgBiodatabase = schema.addTable(metadataStore, "SG_BIODATABASE.csv", "", new DefaultLocation());
        for (int i = 0; i < 8; i++) {
            sgBiodatabase.addColumn(
                    metadataStore,
                    String.format("[%d]", i), // MDMS style
                    "",
                    i
            );
        }
        Table sgBioentry = schema.addTable(metadataStore, "SG_BIOENTRY.csv", "", new DefaultLocation());
        for (int i = 0; i < 8; i++) {
            sgBioentry.addColumn(
                    metadataStore,
                    String.format("[%d]", i), // MDMS style
                    "",
                    i
            );
        }
        Table sgTerm = schema.addTable(metadataStore, "SG_TERM.csv", "", new DefaultLocation());
        for (int i = 0; i < 6; i++) {
            sgTerm.addColumn(
                    metadataStore,
                    String.format("[%d]", i), // MDMS style
                    "",
                    i
            );
        }
        Table sgOntology = schema.addTable(metadataStore, "SG_ONTOLOGY.csv", "", new DefaultLocation());
        for (int i = 0; i < 1; i++) {
            sgOntology.addColumn(
                    metadataStore,
                    String.format("[%d]", i), // MDMS style
                    "",
                    i
            );
        }

        File inputFile = new File(Thread.currentThread().getContextClassLoader().getResource("metanome-inds.json").toURI());

        MetanomeDependencyImportApp.fromParameters(
                metadataStore,
                inputFile.getAbsolutePath(),
                ".+",
                "json",
                "IND",
                "SG",
                "SG",
                null
        );

        Collection<ConstraintCollection<InclusionDependency>> constraintCollections =
                metadataStore.getConstraintCollectionByConstraintTypeAndScope(
                        InclusionDependency.class, metadataStore.getTargetByName("SG")
                );
        Assert.assertEquals(1, constraintCollections.size());
        ConstraintCollection<InclusionDependency> constraintCollection = constraintCollections.iterator().next();
        Assert.assertEquals(2, constraintCollection.getConstraints().size());

        Set<InclusionDependency> discoveredInds = new HashSet<>(constraintCollection.getConstraints());
        Set<InclusionDependency> expectedInds = new HashSet<>();
        expectedInds.add(new InclusionDependency(
                ReferenceUtils.toIntArray(new Column[]{
                        sgBiodatabase.getColumnByName("[0]"),
                        sgBiodatabase.getColumnByName("[2]"),
                        sgBiodatabase.getColumnByName("[3]"),
                        sgBiodatabase.getColumnByName("[4]"),
                        sgBiodatabase.getColumnByName("[5]")
                }),
                ReferenceUtils.toIntArray(new Column[]{
                        sgBioentry.getColumnByName("[7]"),
                        sgBioentry.getColumnByName("[0]"),
                        sgBioentry.getColumnByName("[1]"),
                        sgBioentry.getColumnByName("[2]"),
                        sgBioentry.getColumnByName("[3]")
                })
        ));
        expectedInds.add(new InclusionDependency(
                ReferenceUtils.toIntArray(new Column[]{sgTerm.getColumnByName("[5]")}),
                ReferenceUtils.toIntArray(new Column[]{sgOntology.getColumnByName("[0]")})
        ));
        Assert.assertEquals(expectedInds, discoveredInds);
    }

    @Test
    public void testImportUccsFromJson() throws Exception {
        DefaultMetadataStore metadataStore = new DefaultMetadataStore();
        Schema schema = metadataStore.addSchema("schema", "", new DefaultLocation());
        Table table = schema.addTable(metadataStore, "table", "", new DefaultLocation());
        Column id = table.addColumn(metadataStore, "id", "", 0);
        Column name = table.addColumn(metadataStore, "name", "", 1);
        Column zip = table.addColumn(metadataStore, "zip", "", 2);

        File inputFile = new File(Thread.currentThread().getContextClassLoader().getResource("metanome-uccs.json").toURI());

        MetanomeDependencyImportApp.fromParameters(
                metadataStore,
                inputFile.getAbsolutePath(),
                ".+",
                "json",
                "UCC",
                "schema",
                "schema",
                null
        );

        Collection<ConstraintCollection<UniqueColumnCombination>> constraintCollections =
                metadataStore.getConstraintCollectionByConstraintTypeAndScope(
                        UniqueColumnCombination.class, metadataStore.getTargetByName("schema.table")
                );
        Assert.assertEquals(1, constraintCollections.size());
        ConstraintCollection<UniqueColumnCombination> constraintCollection = constraintCollections.iterator().next();
        Assert.assertEquals(2, constraintCollection.getConstraints().size());

        Set<UniqueColumnCombination> discoveredInds = new HashSet<>(constraintCollection.getConstraints());
        Set<UniqueColumnCombination> expectedInds = new HashSet<>();
        expectedInds.add(new UniqueColumnCombination(ReferenceUtils.toIntArray(new Column[]{id})));
        expectedInds.add(new UniqueColumnCombination(ReferenceUtils.toIntArray(new Column[]{name, zip})));
        Assert.assertEquals(expectedInds, discoveredInds);
    }

}
package de.hpi.isg.mdms.java.util;

import de.hpi.isg.mdms.java.feature.Feature;
import de.hpi.isg.mdms.java.feature.Feature.FeatureType;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Lan Jiang
 */
public class WekaConverter {

    private Dataset dataset;

    private String relationName;

    private String fileName;

    public WekaConverter(Dataset dataset, String relationName) {
        this.dataset = dataset;
        this.relationName = relationName;
        this.fileName = relationName+".arff";
    }

    public void writeDataIntoFile() throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter(fileName));

//        String line = "@relation trainSet";
        String line = "@relation " + relationName;
        bw.write(line);
        bw.newLine();
        bw.newLine();

        convertAttributes(bw);
        convertInstances(bw);

        bw.flush();
        bw.close();
    }

    public void convertAttributes(BufferedWriter bw) throws IOException {
        for (Feature feature : dataset.getFeatures()) {
            String featureName = feature.getFeatureName();
            String line = "@attribute " + featureName;
            if (feature.getFeatureType() == FeatureType.Nominal) {
                line += " {";
                Set<Object> possibles = findPossibleOfNominal(dataset, feature);
                for (Object possible : possibles) {
                    line += possible + ",";
                }
                line = line.substring(0,line.length()-1) + "}";
            }
            else if (feature.getFeatureType() == FeatureType.Numeric) {
                line += " numeric";
            }
            else if (feature.getFeatureType() == FeatureType.String) {
                line += " string";
            }
            bw.write(line);
            bw.newLine();
        }
        String line = "@attribute foreignKey {";
        for (Instance.Result result : Instance.Result.values()) {
            line += result.toString()+",";
        }
        line = line.substring(0,line.length()-1)+"}";
        bw.write(line);
        bw.newLine();
        bw.newLine();
    }

    public void convertInstances(BufferedWriter bw) throws IOException {
        String line = "@data";
        bw.write(line);
        bw.newLine();

        List<Instance> instances = dataset.getDataset();
        for (Instance instance : instances) {
            line = "";
            for (Feature feature : dataset.getFeatures()) {
                line += instance.getFeatureVector().get(feature.getFeatureName()) + ",";
            }
            line += instance.getIsForeignKey().toString();
            bw.write(line);
            bw.newLine();
        }
    }

    public Set<Object> findPossibleOfNominal(Dataset dataset, Feature feature) {
        Set<Object> possibles = new HashSet<>();
        for (Instance instance : dataset.getDataset()) {
            possibles.add(instance.getFeatureVector().get(feature.getFeatureName()));
        }
        return possibles;
    }

    public String getFileName() {
        return fileName;
    }
}

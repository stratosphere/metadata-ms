package de.hpi.isg.mdms.java.fk.ml.evaluation;

import de.hpi.isg.mdms.java.fk.Instance;
import de.hpi.isg.mdms.java.fk.UnaryForeignKeyCandidate;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FMeasureEvaluation {

    protected Map<UnaryForeignKeyCandidate, Instance.Result> groundTruth;

    protected Map<UnaryForeignKeyCandidate, Instance.Result> predicted;

    private Instance.Result evaluatedLabel;

    private double beta;

    public FMeasureEvaluation(Instance.Result evaluatedLabel, double beta) {
        this.evaluatedLabel = evaluatedLabel;
        this.beta = beta;
    }

    public void setGroundTruth(Map<UnaryForeignKeyCandidate, Instance.Result> groundTruth) {
        this.groundTruth = groundTruth;
    }

    public void setPredicted(Map<UnaryForeignKeyCandidate, Instance.Result> predicted) {
        this.predicted = predicted;
    }

    public double evaluate() {
        if (!checkIndentity()) return 0.0;

        return fscore(beta);
    }

    public double precision() {
        List<UnaryForeignKeyCandidate> predictedPositive = predicted.entrySet().stream()
                .filter(unaryForeignKeyCandidateResultEntry -> unaryForeignKeyCandidateResultEntry.getValue()==evaluatedLabel)
                .map(unaryForeignKeyCandidateResultEntry -> unaryForeignKeyCandidateResultEntry.getKey())
                .collect(Collectors.toList());
        long truePositive = groundTruth.entrySet().stream()
                .filter(unaryForeignKeyCandidateResultEntry -> predictedPositive.contains(unaryForeignKeyCandidateResultEntry.getKey()))
                .filter(unaryForeignKeyCandidateResultEntry -> unaryForeignKeyCandidateResultEntry.getValue()==evaluatedLabel)
                .count();
        return (double)truePositive / (double)predictedPositive.size();
    }

    public double recall() {
        List<UnaryForeignKeyCandidate> actualPositive = groundTruth.entrySet().stream()
                .filter(unaryForeignKeyCandidateResultEntry -> unaryForeignKeyCandidateResultEntry.getValue()==evaluatedLabel)
                .map(unaryForeignKeyCandidateResultEntry -> unaryForeignKeyCandidateResultEntry.getKey())
                .collect(Collectors.toList());
        long truePositive = predicted.entrySet().stream()
                .filter(unaryForeignKeyCandidateResultEntry -> actualPositive.contains(unaryForeignKeyCandidateResultEntry.getKey()))
                .filter(unaryForeignKeyCandidateResultEntry -> unaryForeignKeyCandidateResultEntry.getValue()==evaluatedLabel)
                .count();
        return (double)truePositive / (double)actualPositive.size();
    }

    public double fscore(double beta) {
        double precision = precision();
        double recall = recall();
        return (1+Math.pow(beta, 2.0))*precision*recall/(Math.pow(beta, 2.0)*precision+recall);
    }


    private boolean checkIndentity() {
        if (groundTruth.size()!=predicted.size()) return false;

        return groundTruth.keySet().stream().allMatch(unaryForeignKeyCandidate -> predicted.keySet().contains(unaryForeignKeyCandidate));
    }
}

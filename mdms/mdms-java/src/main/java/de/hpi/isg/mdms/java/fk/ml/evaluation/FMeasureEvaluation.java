package de.hpi.isg.mdms.java.fk.ml.evaluation;

import de.hpi.isg.mdms.java.fk.Instance;
import de.hpi.isg.mdms.java.fk.UnaryForeignKeyCandidate;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FMeasureEvaluation extends ClassifierEvaluation{


    private Instance.Result evaluatedLabel;

    private double beta;

    private double fscore;

    public FMeasureEvaluation(Instance.Result evaluatedLabel, double beta) {
        this.evaluatedLabel = evaluatedLabel;
        this.beta = beta;
    }

    @Override
    public void evaluate() {
        if (!checkIndentity()) return;

        fscore(beta);
    }

    @Override
    public Object getEvaluation() {
        return this.fscore;
    }

    private double precision() {
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

    private double recall() {
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

    private void fscore(double beta) {
        double precision = precision();
        double recall = recall();
        fscore = (1+Math.pow(beta, 2.0))*precision*recall/(Math.pow(beta, 2.0)*precision+recall);
    }

    private boolean checkIndentity() {
        if (groundTruth.size()!=predicted.size()) return false;

        return groundTruth.keySet().stream().allMatch(unaryForeignKeyCandidate -> predicted.keySet().contains(unaryForeignKeyCandidate));
    }
}

package de.hpi.isg.mdms.java.fk.ml.evaluation;

import de.hpi.isg.mdms.java.fk.Instance;
import de.hpi.isg.mdms.java.fk.UnaryForeignKeyCandidate;

import java.util.Map;

/**
 * Created by jianghm on 2016/10/24.
 */
abstract public class ClassifierEvaluation {

    protected Map<UnaryForeignKeyCandidate, Instance.Result> groundTruth;

    protected Map<UnaryForeignKeyCandidate, Instance.Result> predicted;


    public void setGroundTruth(Map<UnaryForeignKeyCandidate, Instance.Result> groundTruth) {
        this.groundTruth = groundTruth;
    }

    public void setPredicted(Map<UnaryForeignKeyCandidate, Instance.Result> predicted) {
        this.predicted = predicted;
    }


    abstract public void evaluate();

    abstract public Object getEvaluation();
}

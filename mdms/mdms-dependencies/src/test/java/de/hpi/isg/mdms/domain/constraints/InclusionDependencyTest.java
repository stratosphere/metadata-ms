package de.hpi.isg.mdms.domain.constraints;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author sebastian.kruse
 * @since 05.08.2015
 */
public class InclusionDependencyTest {

  @Test
  public void testImpliedBy() {
    // Create test dependencies.
    InclusionDependency ind0 = new InclusionDependency(new InclusionDependency.Reference(new int[]{0, 1, 1, 1, 2}, new int[]{6, 5, 7, 9, 8}));
    InclusionDependency ind1 = new InclusionDependency(new InclusionDependency.Reference(new int[]{0, 1, 1, 2}, new int[]{6, 7, 9, 8}));
    InclusionDependency ind2 = new InclusionDependency(new InclusionDependency.Reference(new int[]{0, 1, 1, 2}, new int[]{6, 5, 7, 8}));
    InclusionDependency ind3 = new InclusionDependency(new InclusionDependency.Reference(new int[]{0, 1, 2}, new int[]{6, 7, 8}));
    InclusionDependency ind4 = new InclusionDependency(new InclusionDependency.Reference(new int[]{0, 1}, new int[]{6, 7}));
    InclusionDependency ind5 = new InclusionDependency(new InclusionDependency.Reference(new int[]{1}, new int[]{7}));
    InclusionDependency ind6 = new InclusionDependency(new InclusionDependency.Reference(new int[]{0}, new int[]{7}));
    InclusionDependency ind7 = new InclusionDependency(new InclusionDependency.Reference(new int[]{}, new int[]{}));
    InclusionDependency[] inds = new InclusionDependency[] { ind0, ind1, ind2, ind3, ind4, ind5, ind6, ind7};

    // Build a solution matrix.
    boolean[][] solutionMatrix = new boolean[inds.length][inds.length];

    // Reflexivity.
    for (int i = 0; i < inds.length; i++) {
      solutionMatrix[i][i] = true;
    }

    // Others.
    solutionMatrix[1][0] = true;
    solutionMatrix[2][0] = true;
    solutionMatrix[3][0] = true;
    solutionMatrix[4][0] = true;
    solutionMatrix[5][0] = true;
    solutionMatrix[7][0] = true;

    solutionMatrix[3][1] = true;
    solutionMatrix[4][1] = true;
    solutionMatrix[5][1] = true;
    solutionMatrix[7][1] = true;

    solutionMatrix[3][2] = true;
    solutionMatrix[4][2] = true;
    solutionMatrix[5][2] = true;
    solutionMatrix[7][2] = true;

    solutionMatrix[4][3] = true;
    solutionMatrix[5][3] = true;
    solutionMatrix[7][3] = true;

    solutionMatrix[5][4] = true;
    solutionMatrix[7][4] = true;

    solutionMatrix[7][5] = true;

    solutionMatrix[7][6] = true;

    // Test against the solution matrix.
    for (int i = 0; i < inds.length; i++) {
      for (int j = 0; j < inds.length; j++) {
        String msg = String.format("Test for INDs %d and %d failed: should be %s", i, j, solutionMatrix[i][j]);
        Assert.assertTrue(msg, solutionMatrix[i][j] == inds[i].isImpliedBy(inds[j]));
      }
    }
  }

  @Test
  public void testIsTrivial() {
    int[] cols1 = new int[] { 1, 2, 3};
    int[] cols2 = new int[] { 2, 3, 1};
    int[] cols3 = new int[0];

    Assert.assertTrue(new InclusionDependency(new InclusionDependency.Reference(cols1, cols1)).isTrivial());
    Assert.assertFalse(new InclusionDependency(new InclusionDependency.Reference(cols1, cols2)).isTrivial());
    Assert.assertTrue(new InclusionDependency(new InclusionDependency.Reference(cols3, cols3)).isTrivial());
  }

}

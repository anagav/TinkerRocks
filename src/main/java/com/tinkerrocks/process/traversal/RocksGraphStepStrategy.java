package com.tinkerrocks.process.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

/**
 * <p>
 * Graph Step strategy.
 * </p>
 * Created by ashishn on 8/15/15.
 */
public class RocksGraphStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {
    private static final RocksGraphStepStrategy INSTANCE = new RocksGraphStepStrategy();


    @SuppressWarnings("unchecked")
    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        if (traversal.getEngine().isComputer())
            return;

        final Step<?, ?> startStep = traversal.getStartStep();
        if (startStep instanceof GraphStep) {
            final GraphStep<?, ?> originalGraphStep = (GraphStep) startStep;
            final RocksGraphStep<?, ?> rocksGraphStep = new RocksGraphStep<>(originalGraphStep);
            TraversalHelper.replaceStep(startStep, (Step) rocksGraphStep, traversal);

            Step<?, ?> currentStep = rocksGraphStep.getNextStep();
            while (true) {
                if (currentStep instanceof HasContainerHolder) {
                    rocksGraphStep.hasContainers.addAll(((HasContainerHolder) currentStep).getHasContainers());
                    currentStep.getLabels().forEach(rocksGraphStep::addLabel);
                    traversal.removeStep(currentStep);
                } else {
                    break;
                }
                currentStep = currentStep.getNextStep();
            }
        }
    }


    public static RocksGraphStepStrategy instance() {
        return INSTANCE;
    }


}

package com.tinkerrocks.process.computer;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.MapReducePool;
import org.apache.tinkerpop.gremlin.process.computer.util.VertexProgramPool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * Created by ashishn on 8/13/15.
 */
public class WorkerPool implements AutoCloseable {
    private static final BasicThreadFactory threadFactoryWorker = new BasicThreadFactory.Builder().namingPattern("tinker-worker-%d").build();

    private final int numberOfWorkers;
    private final ExecutorService workerPool;

    private VertexProgramPool vertexProgramPool;
    private MapReducePool mapReducePool;

    public WorkerPool(final int numberOfWorkers) {
        this.numberOfWorkers = numberOfWorkers;
        workerPool = Executors.newFixedThreadPool(numberOfWorkers, threadFactoryWorker);
    }

    public void setVertexProgram(final VertexProgram vertexProgram) {
        this.vertexProgramPool = new VertexProgramPool(vertexProgram, this.numberOfWorkers);
    }

    public void setMapReduce(final MapReduce mapReduce) {
        this.mapReducePool = new MapReducePool(mapReduce, this.numberOfWorkers);
    }

    ////

    public void vertexProgramWorkerIterationStart(final Memory memory) {
        this.vertexProgramPool.workerIterationStart(memory);
    }

    public void vertexProgramWorkerIterationEnd(final Memory memory) {
        this.vertexProgramPool.workerIterationEnd(memory);
    }

    public void executeVertexProgram(final Consumer<VertexProgram> worker) {
        try {
            this.workerPool.submit(() -> {
                final VertexProgram vp = this.vertexProgramPool.take();
                worker.accept(vp);
                this.vertexProgramPool.offer(vp);
            }).get();
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    ///

    public void mapReduceWorkerStart(final MapReduce.Stage stage) {
        this.mapReducePool.workerStart(stage);
    }

    public void mapReduceWorkerEnd(final MapReduce.Stage stage) {
        this.mapReducePool.workerEnd(stage);
    }


    public void executeMapReduce(final Consumer<MapReduce> worker) {
        try {
            this.workerPool.submit(() -> {
                final MapReduce mr = this.mapReducePool.take();
                worker.accept(mr);
                this.mapReducePool.offer(mr);
            }).get();
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void close() throws Exception {
        workerPool.shutdown();
    }
}

package util;

import org.apache.spark.util.AccumulatorV2;

public class CollectionAccumulator extends AccumulatorV2<Integer, Integer> {
    private Integer counter = 0;

    @Override
    public boolean isZero() {
        return counter==0;
    }

    @Override
    public AccumulatorV2<Integer, Integer> copy() {
        CollectionAccumulator newAccumulator = new CollectionAccumulator();
        newAccumulator.counter = this.counter;
        return newAccumulator;
    }

    @Override
    public void reset() {
        counter = 0;
    }

    @Override
    public void add(Integer v) {
        counter += v;
    }

    @Override
    public void merge(AccumulatorV2<Integer, Integer> other) {
        counter += other.value();
    }

    @Override
    public Integer value() {
        return counter;
    }
}
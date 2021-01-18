package com.microsoft.utils;

/**
 * @author Jenny.D
 * @create 2021-01-05 18:14
 */
public class RanOpt<T> {
    private T value;
    private int weight;

    public RanOpt(T value, int weight) {
        this.value = value;
        this.weight = weight;
    }

    public T getValue() {
        return value;
    }

    public int getWeight() {
        return weight;
    }

}

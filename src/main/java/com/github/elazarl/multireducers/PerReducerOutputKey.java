package com.github.elazarl.multireducers;

/**
 * PerReducerOutputValue would mark a value as designated to a specific
 * output format.
 */
public class PerReducerOutputKey extends PerInternalMapper {
    public PerReducerOutputKey() {
    }

    public PerReducerOutputKey(int targetReducer, Object data) {
        super(targetReducer, data);
    }
}

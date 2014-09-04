package com.akamai.csi.multireducers;

/**
 * PerReducerWritable would contain a writable, and a byte
 * specifying to which reducer should this writable reach.
 */
public class PerMapperOutputKey extends PerInternalMapper {
    PerMapperOutputKey(){}

    PerMapperOutputKey(int targetReducer, Object data) {
        super(targetReducer, data);
    }
}

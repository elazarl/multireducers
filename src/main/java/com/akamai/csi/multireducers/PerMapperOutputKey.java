package com.akamai.csi.multireducers;

/**
 * PerMapperOutputKey would contain an object, and a byte
 * specifying to which reducer should this writable reach.
 */
public class PerMapperOutputKey extends PerInternalMapper {
    PerMapperOutputKey(){}

    PerMapperOutputKey(int targetReducer, Object data) {
        super(targetReducer, data);
    }
}

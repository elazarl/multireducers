package com.akamai.csi.multireducers;

/**
 * PerMapperOutputValue would contain a mapper output value, and a byte
 * specifying to which reducer should this writable reach.
 * TODO(elazar): remove that, and have MR record reader understand that from the key
 */
public class PerMapperOutputValue extends PerInternalMapper {
    @SuppressWarnings("unused")
    PerMapperOutputValue(){}
    PerMapperOutputValue(int targetReducer, Object data) {
        super(targetReducer, data);
    }
}

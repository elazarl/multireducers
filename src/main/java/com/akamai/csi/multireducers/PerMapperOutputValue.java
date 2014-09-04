package com.akamai.csi.multireducers;

/**
 * PerMapperOutputValue keeps value per mapper.
 * TODO(elazar): remove that, and have MR record reader understand that from the key
 */
public class PerMapperOutputValue extends PerInternalMapper {
    @SuppressWarnings("unused")
    PerMapperOutputValue(){}
    PerMapperOutputValue(int targetReducer, Object data) {
        super(targetReducer, data);
    }
}

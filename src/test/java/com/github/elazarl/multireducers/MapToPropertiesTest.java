package com.github.elazarl.multireducers;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class MapToPropertiesTest {

    @Test
    public void testDeserialize() throws Exception {
        String s = MapToProperties.serialize(ImmutableMap.of("a", "1", "b", "2"));
        assertThat(MapToProperties.deserialize(s), is((Map) ImmutableMap.of("a", "1", "b", "2")));
    }

    @Test
    public void testSpecialCharacters() throws Exception {
        String s = MapToProperties.serialize(ImmutableMap.of("a,=", "1", "b", "2,=;"));
        assertThat(MapToProperties.deserialize(s), is((Map) ImmutableMap.of("a,=", "1", "b", "2,=;")));
    }
}
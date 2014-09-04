package com.akamai.csi.multireducers;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;
import java.util.Collection;

/**
 * MultiSerializer serializes PerReducerData
 */
public class MultiSerializer implements Serialization<PerInternalMapper>, Configurable {

    @Override
    public boolean accept(Class c) {
        return c.equals(PerMapperOutputKey.class) || c.equals(PerMapperOutputValue.class);
    }

    @Override
    public Serializer<PerInternalMapper> getSerializer(Class c) {
        Class[] serClasses = (c.equals(PerMapperOutputKey.class)) ?
                mapperOutputKeyClasses : mapperOutputValueClasses;
        return new PerInternalMapperSerializer(factory, serClasses);
    }

    @Override
    public Deserializer<PerInternalMapper> getDeserializer(final Class c) {
        Class[] serClasses = (c.equals(PerMapperOutputKey.class)) ?
                mapperOutputKeyClasses : mapperOutputValueClasses;
        return new PerInternalMapperDeserializer(factory, serClasses, c,  conf);

    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        mapperOutputKeyClasses = conf.getClasses(MultiReducer.INPUT_KEY_CLASSES);
        mapperOutputValueClasses = conf.getClasses(MultiReducer.INPUT_VALUE_CLASSES);
        factory = new SerializationFactory(removeMyClass(conf));
    }

    private Configuration removeMyClass(Configuration conf) {
        Configuration withoutMe = new Configuration(conf);
        Collection<String> classes = withoutMe.getStringCollection(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY);
        classes.remove(MultiSerializer.class.getName());
        withoutMe.setStrings(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY,
                classes.toArray(new String[classes.size()]));
        return withoutMe;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    private Configuration conf;

    Class[] mapperOutputKeyClasses;
    Class[] mapperOutputValueClasses;
    SerializationFactory factory;

    private static class PerInternalMapperSerializer implements Serializer<PerInternalMapper> {
        private final Serializer[] serializers;
        private OutputStream nopClose;
        private DataOutputStream dataOut;
        private Serializer[] serializerUsed;

        public PerInternalMapperSerializer(SerializationFactory factory, Class[] serClasses) {
            this.serializers = new Serializer[serClasses.length];
            for (int i = 0; i < serializers.length; i++) {
                serializers[i] = factory.getSerializer(serClasses[i]);
            }
            serializerUsed = new Serializer[serializers.length];
        }

        @Override
        public void open(OutputStream out) throws IOException {
            this.nopClose = new NopCloseOutputStream(out);
            if (out instanceof DataOutputStream) {
                dataOut = (DataOutputStream) out;
            } else {
                dataOut = new DataOutputStream(out);
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public void serialize(PerInternalMapper perInternalMapper) throws IOException {
            WritableUtils.writeVInt(dataOut, perInternalMapper.targetReducer);
            getSerializer(perInternalMapper.targetReducer).serialize(perInternalMapper.data);
        }

        private Serializer getSerializer(int i) throws IOException {
            if (serializerUsed[i] == null) {
                serializerUsed[i] = serializers[i];
                serializerUsed[i].open(nopClose);
            }
            return serializerUsed[i];
        }

        @Override
        public void close() throws IOException {
            for (Serializer serializer : serializerUsed) {
                if (serializer != null) {
                    serializer.close();
                }
            }
            dataOut.close();
        }
    }

    private static class PerInternalMapperDeserializer implements Deserializer<PerInternalMapper> {
        private final Deserializer[] deserializers;
        private final Class[] serClasses;
        private Class c;
        private Configuration conf;
        InputStream nopClose;
        DataInputStream dataIn;
        private Deserializer[] deserializersUsed;

        public PerInternalMapperDeserializer(SerializationFactory factory, Class[] serClasses, Class c, Configuration conf) {
            this.serClasses = serClasses;
            this.deserializers = new Deserializer[serClasses.length];
            for (int i = 0; i < deserializers.length; i++) {
                deserializers[i] = factory.getDeserializer(serClasses[i]);
            }
            this.c = c;
            this.conf = conf;
            deserializersUsed = new Deserializer[deserializers.length];
        }

        @Override
        public void open(InputStream in) throws IOException {
            nopClose = new NopCloseInputStream(in);
            if (in instanceof DataInputStream) {
                dataIn = (DataInputStream) in;
            } else {
                dataIn = new DataInputStream(in);
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public PerInternalMapper deserialize(PerInternalMapper m) throws IOException {
            if (m == null) {
                m = (PerInternalMapper) ReflectionUtils.newInstance(c, conf);
            }
            m.targetReducer = WritableUtils.readVInt(dataIn);
            if (!serClasses[m.targetReducer].isInstance(m.data)) {
                // TODO: cache perInternalMapper per targetReducer
                m.data = ReflectionUtils.newInstance(serClasses[m.targetReducer], conf);
            }
            m.data = getDesiralizer(m.targetReducer).deserialize(m.data);
            return m;
        }

        @Override
        public void close() throws IOException {
            for (Deserializer deserializer : deserializersUsed) {
                if (deserializer != null) {
                    deserializer.close();
                }
            }
            dataIn.close();
        }

        private Deserializer getDesiralizer(int i) throws IOException {
            if (deserializersUsed[i] == null) {
                deserializersUsed[i] = deserializers[i];
                deserializersUsed[i].open(nopClose);
            }
            return deserializersUsed[i];
        }
    }
}

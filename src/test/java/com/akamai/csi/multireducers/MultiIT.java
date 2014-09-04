package com.akamai.csi.multireducers;

import com.akamai.csi.multireducers.example.ExampleRunner;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import com.google.common.io.*;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;
import org.apache.hadoop.mapred.JobConf;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.EnumSet;
import java.util.Properties;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * Run example job on a mini cluster
 */
public class MultiIT extends ClusterMapReduceTestCase {

    @Override
    protected void setUp() throws Exception {
        File logDir = new File("/tmp/logs");
        FileUtils.deleteDirectory(logDir);
        assertThat(logDir.mkdirs(), is(true));
        System.setProperty("hadoop.log.dir", "/tmp/logs");
        Properties properties = new Properties();
        properties.setProperty("io.sort.mb", "1");
        properties.setProperty("io.sort.spill.percent", "0.0000001");
        super.startCluster(true, properties);
    }

    public void testExample() throws Exception {

        ExampleRunner exampleRunner = new ExampleRunner();
        JobConf conf = createJobConf();
        conf.setNumReduceTasks(10);
        exampleRunner.setConf(conf);
        final FileContext fc = FileContext.getFileContext(conf);

        fc.mkdir(getInputDir(), FsPermission.getDefault(), true);
        Path inputFile = new Path(getInputDir(), "input.txt");

        int times = 1024 * 1024 + 1;
        createInputFile(fc.create(inputFile, EnumSet.of(CreateFlag.CREATE)), times);
        assertThat(exampleRunner.run(new String[]{getTestRootDir() + inputFile.toString(), getTestRootDir() + getOutputDir().toString()}),
                is(0));
        RemoteIterator<FileStatus> it = fc.listStatus(new Path(getOutputDir(), "output"));

        Multiset<String> countFirst = HashMultiset.create();
        Multiset<String> countSecond = HashMultiset.create();

        while (it.hasNext()) {
            final Path path = it.next().getPath();
            Multiset<String> map = MultiJobTest.toMap(new InputSupplier<InputStream>() {
                @Override
                public InputStream getInput() throws IOException {
                    return fc.open(path);
                }
            });
            if (path.getName().startsWith("CountFirst")) {
                countFirst.addAll(map);
            }
            if (path.getName().startsWith("CountSecond")) {
                countSecond.addAll(map);
            }
        }
        assertThat(countFirst, notNullValue());
        assertThat(countSecond, notNullValue());
        assert(countFirst != null && countSecond != null);

        assertThat(ImmutableMultiset.copyOf(countFirst), is(new ImmutableMultiset.Builder<String>().
                addCopies("john", 2 * times).
                addCopies("dough", times).
                addCopies("joe", times).
                addCopies("moe", times).build()));
        assertThat(ImmutableMultiset.copyOf(countSecond), is(new ImmutableMultiset.Builder<String>().
                addCopies("120", times).
                addCopies("130", 2*times).
                addCopies("180", times).
                addCopies("190", times).build()));
    }

    private void createInputFile(OutputStream out, int times) throws IOException {
        try {
            URL inputResource = getClass().getClassLoader().getResource("example_input.txt");
            assert(inputResource != null);
            byte[] buf = ByteStreams.toByteArray(Resources.newInputStreamSupplier(inputResource));
            for (int i=0; i< times; i++) {
                out.write(buf);
            }
        } finally {
            out.close();
        }
    }
}

package com.akamai.csi.multireducers;

import com.akamai.csi.multireducers.example.*;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.common.io.Resources;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;
import java.util.Scanner;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;

/**
 * Test a full flow of an example MultiJob with the job local runner
 */
public class MultiJobTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public ExpectedException exception = ExpectedException.none();


    @Test
    public void testBadCombinerConfiguration() throws Exception {
        exception.expect(IllegalArgumentException.class);
        Job job = new Job();
        MultiJob.create().
                withMapper(SelectFirstField.class, Text.class, IntWritable.class).
                withReducer(CountFirstField.class, 1).
                withCombiner(CountFirstField.class).
                withOutputFormat(TextOutputFormat.class, Text.class, IntWritable.class).
                addTo(job);
        MultiJob.create().
                withMapper(SelectSecondField.class, IntWritable.class, IntWritable.class).
                withReducer(CountSecondField.class, 1).
                withCombiner(CountFirstField.class).
                withOutputFormat(TextOutputFormat.class, Text.class, IntWritable.class).
                addTo(job);
    }

    @Test
    public void testBadReducerConfiguration() throws Exception {
        exception.expect(IllegalArgumentException.class);
        Job job = new Job();
        MultiJob.create().
                withMapper(SelectFirstField.class, Text.class, IntWritable.class).
                withReducer(CountFirstField.class, 1).
                withCombiner(CountFirstField.class).
                withOutputFormat(TextOutputFormat.class, Text.class, IntWritable.class).
                addTo(job);
        MultiJob.create().
                withMapper(SelectSecondField.class, IntWritable.class, IntWritable.class).
                withReducer(CountFirstField.class, 1).
                withCombiner(CountSecondField.class).
                withOutputFormat(TextOutputFormat.class, Text.class, IntWritable.class).
                addTo(job);
    }

    static class MapperValueText extends Reducer<Object, Text, Object, Object>{
        @Override
        protected void reduce(Object key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        }
    }
    @Test
    public void testBadReducerValueConfiguration() throws Exception {
        Job job = new Job();
        MultiJob.create().
                withMapper(SelectSecondField.class, IntWritable.class, IntWritable.class).
                withReducer(MapperValueText.class, 1).
                withCombiner(CountSecondField.class).
                withOutputFormat(TextOutputFormat.class, Text.class, IntWritable.class).
                skipJobVerificationCanCauseRuntimeErrorsIKnowWhatImDoing().
                addTo(job);
        exception.expect(IllegalArgumentException.class);
        MultiJob.create().
                withMapper(SelectFirstField.class, Text.class, IntWritable.class).
                withReducer(CountFirstField.class, 1).
                withCombiner(CountFirstField.class).
                withOutputFormat(TextOutputFormat.class, Text.class, IntWritable.class).
                addTo(job);
        MultiJob.create().
                withMapper(SelectSecondField.class, IntWritable.class, IntWritable.class).
                withReducer(MapperValueText.class, 1).
                withCombiner(CountSecondField.class).
                withOutputFormat(TextOutputFormat.class, Text.class, IntWritable.class).
                addTo(job);
    }

    static class InheritCountFirstField extends CountFirstField{}
    @Test
    public void testBadReducerKeyByInheriance() throws Exception {
        exception.expect(IllegalArgumentException.class);
        Job job = new Job();
        MultiJob.create().
                withMapper(SelectFirstField.class, Text.class, IntWritable.class).
                withReducer(CountFirstField.class, 1).
                withCombiner(CountFirstField.class).
                withOutputFormat(TextOutputFormat.class, Text.class, IntWritable.class).
                addTo(job);
        MultiJob.create().
                withMapper(SelectSecondField.class, IntWritable.class, IntWritable.class).
                withReducer(InheritCountFirstField.class, 1).
                withCombiner(CountSecondField.class).
                withOutputFormat(TextOutputFormat.class, Text.class, IntWritable.class).
                addTo(job);
    }

    @Test
    public void testExampleJob() throws Exception {
        File input = createInputFile();
        File output = new File(folder.getRoot(), "output");
        int exitCode = ToolRunner.run(new ExampleRunner(), new String[]{"file://" + input.getAbsolutePath(),
                "file://" + output.getAbsolutePath()});
        assertThat(exitCode, is(0));
        File[] firstFieldFiles = output.listFiles((FilenameFilter) new WildcardFileFilter("CountFirstField*"));
        File[] secondFieldFiles = output.listFiles((FilenameFilter) new WildcardFileFilter("CountSecondField*"));
        assertThat(firstFieldFiles.length, is(1));
        assertThat(firstFieldFiles[0].length(), greaterThan(0l));
        assertThat(secondFieldFiles.length, is(1));
        assertThat(secondFieldFiles[0].length(), greaterThan(0l));
        Map<String, Integer> countFirstField = toMap(Files.newInputStreamSupplier(firstFieldFiles[0]));
        Map<String, Integer> countSecondField = toMap(Files.newInputStreamSupplier(secondFieldFiles[0]));
        assertThat(ImmutableMap.copyOf(countFirstField), is(ImmutableMap.of(
                "john", 2,
                "dough", 1,
                "joe", 1,
                "moe", 1)));
        assertThat(ImmutableMap.copyOf(countSecondField), is(ImmutableMap.of(
                "120", 1,
                "130", 2,
                "180", 1,
                "190", 1)));
    }

    public static Map<String, Integer> toMap(InputSupplier<? extends InputStream> supplier) throws IOException {
        InputStream input = supplier.getInput();
        try {
            Scanner scanner = new Scanner(input);
            Map<String, Integer> m = Maps.newHashMap();
            while (scanner.hasNext()) {
                String line = scanner.nextLine();
                String[] parts = line.split("\t");
                m.put(parts[0], Integer.parseInt(parts[1]));
            }
            return m;
        } finally {
            input.close();
        }
    }

    private File createInputFile() throws IOException {
        File input = folder.newFile("input.txt");
        URL inputResource = getClass().getClassLoader().getResource("example_input.txt");
        assert(inputResource != null);
        ByteStreams.copy(Resources.newInputStreamSupplier(inputResource),
                Files.newOutputStreamSupplier(input));
        return input;
    }
}

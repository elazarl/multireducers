Multireducers for Hadoop
========================

Run multiple logical mapreduce jobs on the same physical mapreduce job.
 
Quick start
-----------

Since this is an alpha release, you must first clone the git project, and run
maven install.

```bash
$ git clone https://github.com/elazarl/multireducers.git
$ cd multireducers
$ mvn clean install
```

In your `pom.xml` file include your hadoop mapreduce client, and the multireducers
project. For example:

```maven
<!-- could be apache's version -->
<dependency>
  <groupId>org.apache.hadoop</groupId>
  <artifactId>hadoop-client</artifactId>
  <version>2.0.0-mr1-cdh4.3.1</version>
</dependency>
<dependency>
  <groupId>com.akamai.csi</groupId>
  <artifactId>multireducers</artifactId>
  <version>0.1-SNAPSHOT</version>
</dependency>
```

Now, write two or more regular Hadoop MapReduce jobs. and use the
`MultiJob.create()` helper method to configure all your job.

You need to set comparator, partitioner, combiner, map class
and reduce class from `MultiJob`. Other configuration, like registering
custom serialization class is done as usual.

```java
MultiJob.create().
        withMapper(SelectFirstField.class, Text.class, IntWritable.class).
        withReducer(CountFirstField.class, 1).
        withCombiner(CountFirstField.class).
        withOutputFormat(TextOutputFormat.class, Text.class, IntWritable.class).
        addTo(job);
MultiJob.create().
        withMapper(SelectSecondField.class, IntWritableInRange.class, IntWritable.class).
        withReducer(CountSecondField.class, 1).
        withCombiner(CountSecondField.class).
        withOutputFormat(TextOutputFormat.class, Text.class, IntWritable.class).
        addTo(job);
```

Then run the job as you usually do.

Example Job
-----------

See an example of
[main class](src/test/java/com/akamai/csi/multireducers/example/ExampleRunner.java)
running 
[first mapper](src/test/java/com/akamai/csi/multireducers/example/SelectFirstField.java)
and [reducer](src/test/java/com/akamai/csi/multireducers/example/CountFirstField.java)
along with
[second mapper](src/test/java/com/akamai/csi/multireducers/example/SelectSecondField.java)
and [reducer](src/test/java/com/akamai/csi/multireducers/example/CountSecondField.java)
each reducer writes to its own file, marked by the reducer's class name and its index
in case there are two identical reducers.

You can run it locally on a mini cluster with the
[MultiIT](src/test/java/com/akamai/csi/multireducers/MultiIT.java) test by running
`mvn verify`, or compile a test jar with `mvn jar:test-jar`, and then run
the example on the cluster with

```bash
$ java -cp `hadoop classpath`:multireducers-0.1-SNAPSHOT.jar:multireducers-0.1-SNAPSHOT-tests.jar \
com.akamai.csi.multireducers.example.ExampleRunner /input/file.txt /output/dir
```

Motivation
----------

Sometimes, one would like to run more than one MapReduce job on the same input files.

A classic example, is one would like to select two different fields from a CSV file
with two different mappers, and count the distinct values for each field.

Let's say we're having a CSV file with employee's names and height

```csv
john,120
john,130
joe,180
moe,190
dough,130
```

We want one MapReduce job to count how many employees we have for each name (two johns
in our cases), and also, how many employees do we have for each height (two employees
130 cm high).

The code for the mappers looks like

```java
// i = 0 for the first reducer, 1 for the second
protected void map(LongWritable key, Text value, Context context) {
    context.write(new Text(value.toString().split(",")[i]), one);
}
```

The code for the reducers looks like

```java
protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
    context.write(key, new IntWritable(Iterables.size(values));
}
```

We can run two MapReduce jobs twice on the same file, but this means we're reading
the file twice from HDFS.

Instead, we can configure with `multireducers` to run both mappers and both reducers
in a single MapReduce job. Both would read the same input, but would write their
results to different reducers and different `OutputFormat`s.

The code for the configuration looks like:

```java
MultiJob.create().
        withMapper(SelectFirstField.class, Text.class, IntWritable.class).
        withReducer(CountFirstField.class, 1).
        withCombiner(CountFirstField.class).
        withOutputFormat(TextOutputFormat.class, Text.class, IntWritable.class).
        addTo(job);
MultiJob.create().
        withMapper(SelectSecondField.class, IntWritableInRange.class, IntWritable.class).
        withReducer(CountSecondField.class, 1).
        withCombiner(CountSecondField.class).
        withOutputFormat(TextOutputFormat.class, Text.class, IntWritable.class).
        addTo(job);
```
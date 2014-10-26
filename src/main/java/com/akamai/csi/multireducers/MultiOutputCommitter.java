package com.akamai.csi.multireducers;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

/**
* MultiOutputCommitter would commit multiple committers together.
*/
class MultiOutputCommitter extends OutputCommitter {

    private List<OutputCommitter> committers;

    public MultiOutputCommitter(List<OutputCommitter> committers) {
        this.committers = committers;
    }

    @Override
    public void setupJob(JobContext jobContext) throws IOException {
        for(OutputCommitter committer: committers){
            committer.setupJob(jobContext);
        }
    }

    @Override
    public void commitJob(JobContext jobContext) throws IOException {
        for(OutputCommitter committer: committers){
            committer.commitJob(jobContext);
        }
    }

    // we need to support deprecated API too...
    @SuppressWarnings("deprecation")
    @Override
    public void cleanupJob(JobContext context) throws IOException {
        for(OutputCommitter committer: committers){
            committer.cleanupJob(context);
        }
    }

    @Override
    public void abortJob(JobContext jobContext, JobStatus.State state)
            throws IOException {
        boolean success = true;

        for(OutputCommitter committer: committers){
            try{
                committer.abortJob(jobContext,state);
            }catch (Exception e) {
                e.printStackTrace();
                success = false;
            }
        }

        if(!success){
            throw new RuntimeException("failures occur while abort job");
        }
    }

    @Override
    public void setupTask(TaskAttemptContext taskContext)
            throws IOException {
        for(OutputCommitter committer: committers){
            committer.setupTask(taskContext);
        }
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext)
            throws IOException {
        return true;
    }

    @Override
    public void commitTask(TaskAttemptContext taskContext)
            throws IOException {
        for(OutputCommitter committer: committers){
            committer.commitTask(taskContext);
        }
    }

    @Override
    public void abortTask(TaskAttemptContext taskContext)
            throws IOException {
        boolean success = true;

        for(OutputCommitter committer: committers){
            try{
                committer.abortTask(taskContext);
            }catch (Exception e) {
                e.printStackTrace();
                success = false;
            }
        }

        if(!success){
            throw new RuntimeException("failures occur while abort task");
        }
    }

}

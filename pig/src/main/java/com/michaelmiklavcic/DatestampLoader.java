package com.michaelmiklavcic;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;

public class DatestampLoader extends LoadFunc {
    protected PigStorage loadFuncDelegate;
    protected String timestamp;
    TimestampParser tsParser;

    public DatestampLoader(String timestampFormat) {
        this(new PigStorage(), timestampFormat);
    }

    public DatestampLoader(PigStorage pigStorage, String timestampFormat) {
        this.loadFuncDelegate = pigStorage;
        this.tsParser = new TimestampParser(timestampFormat);
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        loadFuncDelegate.setLocation(location, job);
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        return loadFuncDelegate.getInputFormat();
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
        loadFuncDelegate.prepareToRead(reader, split);
        String fileName = getPath(split).getName();
        timestamp = tsParser.parseFrom(fileName);
    }

    protected Path getPath(PigSplit split) {
        return ((FileSplit) split.getWrappedSplit()).getPath();
    }

    @Override
    public Tuple getNext() throws IOException {
        Tuple tuple = loadFuncDelegate.getNext();
        if (tuple != null) {
            tuple.append(timestamp);
        }
        return tuple;
    }

    public String relativeToAbsolutePath(String location, Path curDir) throws IOException {
        return loadFuncDelegate.relativeToAbsolutePath(location, curDir);
    }

    public LoadCaster getLoadCaster() throws IOException {
        return loadFuncDelegate.getLoadCaster();
    }

    public void setUDFContextSignature(String signature) {
        loadFuncDelegate.setUDFContextSignature(signature);
    }

}

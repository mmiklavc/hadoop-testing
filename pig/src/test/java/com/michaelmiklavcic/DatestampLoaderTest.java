package com.michaelmiklavcic;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class DatestampLoaderTest {
    @Mock private PigStorage pigStorage;
    @Mock private Tuple tuple;
    @Mock private Tuple tuple2;
    @Mock private PigSplit pigSplit;
    @Mock private FileSplit fileSplit;
    @Mock private RecordReader reader;
    @Mock private Path path;
    private DatestampLoader storage;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        storage = new DatestampLoader(pigStorage, "yyyy-MM-dd_HH-mm-ss");
    }

    @Test
    public void sinlge_option_constructor_set_defaults() {
        storage = new DatestampLoader("yyyy");
        assertThat(storage.loadFuncDelegate, instanceOf(PigStorage.class));
        assertThat(storage.tsParser, instanceOf(TimestampParser.class));
    }

    @Test
    public void getNext_appends_timestamp_as_last_column() throws Exception {
        storage.timestamp = "2014-11-01_14-21-44";
        when(pigStorage.getNext()).thenReturn(tuple);
        storage.getNext();
        verify(tuple).append("2014-11-01_14-21-44");
    }

    @Test
    public void getNext_returns_null_when_records_exhausted() throws Exception {
        when(pigStorage.getNext()).thenReturn(tuple, tuple2, null);
        assertThat(storage.getNext(), equalTo(tuple));
        assertThat(storage.getNext(), equalTo(tuple2));
        assertThat(storage.getNext(), equalTo(null));
    }

    @Test
    public void getSplit_returns_file_split() {
        when(pigSplit.getWrappedSplit()).thenReturn(fileSplit);
        when(fileSplit.getPath()).thenReturn(path);
        assertThat(storage.getPath(pigSplit), sameInstance(path));
    }

    @Test
    public void prepareToRead_parses_timestamp_from_filename() throws IOException {
        when(pigSplit.getWrappedSplit()).thenReturn(fileSplit);
        when(fileSplit.getPath()).thenReturn(path);
        when(path.getName()).thenReturn("2014-11-01_14-21-44-file.csv");
        // when(timestampParser.parseFrom("2014-11-01_09-21-44-file.csv")).thenReturn("2014-11-01_09-21-44");
        storage.prepareToRead(reader, pigSplit);
        assertThat(storage.timestamp, equalTo("2014-11-01_14-21-44"));
    }

}

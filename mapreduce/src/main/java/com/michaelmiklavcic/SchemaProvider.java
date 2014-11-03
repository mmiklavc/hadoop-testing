package com.michaelmiklavcic;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

public interface SchemaProvider {
    public HCatSchema getSchema(Configuration conf) throws IOException;
}

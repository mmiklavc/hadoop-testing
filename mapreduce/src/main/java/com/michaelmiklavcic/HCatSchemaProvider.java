package com.michaelmiklavcic;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatBaseInputFormat;

public class HCatSchemaProvider implements SchemaProvider {

    @Override
    public HCatSchema getSchema(Configuration conf) throws IOException {
        return HCatBaseInputFormat.getTableSchema(conf);
    }

}

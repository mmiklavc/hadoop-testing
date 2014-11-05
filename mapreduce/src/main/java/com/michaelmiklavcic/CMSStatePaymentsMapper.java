package com.michaelmiklavcic;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

public class CMSStatePaymentsMapper extends Mapper<WritableComparable<LongWritable>, HCatRecord, Text, Text> {

    private SchemaProvider schemaProvider = new HCatSchemaProvider();

    @Override
    protected void map(WritableComparable<LongWritable> key, HCatRecord value, Context context) throws java.io.IOException, InterruptedException {
        HCatSchema schema = schemaProvider.getSchema(context.getConfiguration());
        String state = value.getString("recipient_state", schema);
        String amount = value.getString("total_amount_of_payment_usdollars", schema);
        context.write(new Text(state), new Text(amount));
    }

    protected void setSchemaProvider(SchemaProvider provider) {
        this.schemaProvider = provider;
    }
}

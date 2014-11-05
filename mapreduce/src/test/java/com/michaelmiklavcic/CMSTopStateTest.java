package com.michaelmiklavcic;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchemaUtils;
import org.junit.Before;
import org.junit.Test;

public class CMSTopStateTest {

    private TestSchemaProvider provider;
    private CMSStatePaymentsMapper mapper;
    private CMSStatePaymentsReducer reducer;
    private MapDriver mapDriver;
    private ReduceDriver reduceDriver;

    @Before
    public void setup() {
        provider = new TestSchemaProvider();
        mapper = new CMSStatePaymentsMapper();
        mapper.setSchemaProvider(provider);
        reducer = new CMSStatePaymentsReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void mapper_emits_values_by_reading_schema_from_hcatalog() throws Exception {
        HCatSchema hCatSchema = buildTestSchema();
        provider.setSchema(hCatSchema);

        // setup hcatrecords. We care most about 15 and 48
        List<DefaultHCatRecord> hCatRecords = new ArrayList<DefaultHCatRecord>();
        for (String[] rec : readFile("src/test/resources/sample.txt")) {
            DefaultHCatRecord hcr = new DefaultHCatRecord(rec.length);
            for (int i = 0; i < rec.length; i++) {
                hcr.set(i, rec[i]);
            }
            hCatRecords.add(hcr);
            mapDriver.withInput(new LongWritable(), hcr);
        }

        mapDriver.withOutput(new Text("CA"), new Text("69"));
        mapDriver.withOutput(new Text("TX"), new Text("14.38"));
        mapDriver.withOutput(new Text("MI"), new Text("13.53"));
        mapDriver.withOutput(new Text("CA"), new Text("18.3"));
        mapDriver.withOutput(new Text("IL"), new Text("16.06"));
        mapDriver.runTest();
    }

    private HCatSchema buildTestSchema() throws HCatException {
        HCatSchema hCatSchema = HCatSchemaUtils.getListSchemaBuilder().build();
        String[] fields = new String[] {
                "general_transaction_id",
                "program_year",
                "payment_publication_date",
                "submitting_applicable_manufacturer_or_applicable_gpo_name",
                "covered_recipient_type",
                "teaching_hospital_id",
                "teaching_hospital_name",
                "physician_profile_id",
                "physician_first_name",
                "physician_middle_name",
                "physician_last_name",
                "physician_name_suffix",
                "recipient_primary_business_street_address_line1",
                "recipient_primary_business_street_address_line2",
                "recipient_city",
                "recipient_state",
                "recipient_zip_code",
                "recipient_country",
                "recipient_province",
                "recipient_postal_code",
                "physician_primary_type",
                "physician_specialty",
                "physician_license_state_code1",
                "physician_license_state_code2",
                "physician_license_state_code3",
                "physician_license_state_code4",
                "physician_license_state_code5",
                "product_indicator",
                "name_of_associated_covered_drug_or_biological1",
                "name_of_associated_covered_drug_or_biological2",
                "name_of_associated_covered_drug_or_biological3",
                "name_of_associated_covered_drug_or_biological4",
                "name_of_associated_covered_drug_or_biological5",
                "ndc_of_associated_covered_drug_or_biological1",
                "ndc_of_associated_covered_drug_or_biological2",
                "ndc_of_associated_covered_drug_or_biological3",
                "ndc_of_associated_covered_drug_or_biological4",
                "ndc_of_associated_covered_drug_or_biological5",
                "name_of_associated_covered_device_or_medical_supply1",
                "name_of_associated_covered_device_or_medical_supply2",
                "name_of_associated_covered_device_or_medical_supply3",
                "name_of_associated_covered_device_or_medical_supply4",
                "name_of_associated_covered_device_or_medical_supply5",
                "applicable_manufacturer_or_applicable_gpo_making_payment_name",
                "applicable_manufacturer_or_applicable_gpo_making_payment_id",
                "applicable_manufacturer_or_applicable_gpo_making_payment_state",
                "applicable_manufacturer_or_applicable_gpo_making_payment_country",
                "dispute_status_for_publication",
                "total_amount_of_payment_usdollars",
                "date_of_payment",
                "number_of_payments_included_in_total_amount",
                "form_of_payment_or_transfer_of_value",
                "nature_of_payment_or_transfer_of_value",
                "city_of_travel",
                "state_of_travel",
                "country_of_travel",
                "physician_ownership_indicator",
                "third_party_payment_recipient_indicator",
                "name_of_third_party_entity_receiving_payment_or_transfer_of_value",
                "charity_indicator",
                "third_party_equals_covered_recipient_indicator",
                "contextual_information",
                "delay_in_publication_of_general_payment_indicator" };
        for (String field : fields) {
            hCatSchema.append(new HCatFieldSchema(field, TypeInfoFactory.stringTypeInfo, ""));
        }
        return hCatSchema;
    }

    @Test
    public void reducer_totals_payments_by_state() throws Exception {
        reduceDriver.withInput(new Text("CA"), Arrays.asList(new Text[] {
                new Text("69"),
                new Text("18.3") }));
        reduceDriver.withInput(new Text("TX"), Arrays.asList(new Text[] { new Text("14.38") }));
        reduceDriver.withInput(new Text("MI"), Arrays.asList(new Text[] { new Text("13.53") }));
        reduceDriver.withInput(new Text("IL"), Arrays.asList(new Text[] { new Text("16.06") }));
        reduceDriver.withOutput(new Text("CA"), new Text("87.3"));
        reduceDriver.withOutput(new Text("TX"), new Text("14.38"));
        reduceDriver.withOutput(new Text("MI"), new Text("13.53"));
        reduceDriver.withOutput(new Text("IL"), new Text("16.06"));
        reduceDriver.runTest();
    }

    private List<String[]> readFile(String path) throws Exception {
        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            String line = "";
            List<String[]> records = new ArrayList<String[]>();
            while ((line = reader.readLine()) != null) {
                records.add(line.split("\u0001"));
            }
            return records;
        }
    }

    public static class TestSchemaProvider implements SchemaProvider {

        private HCatSchema schema;

        @Override
        public HCatSchema getSchema(Configuration conf) {
            return schema;
        }

        public void setSchema(HCatSchema schema) {
            this.schema = schema;
        }

    }
}

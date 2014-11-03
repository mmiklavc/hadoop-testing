package com.michaelmiklavcic;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.*;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.schema.*;
import org.junit.*;

public class CMSTopStateTest {

    private TestSchemaProvider provider;
    private CMSTopStateMapper mapper;
    private CMSTopStateReducer reducer;
    private MapDriver mapDriver;
    private ReduceDriver reduceDriver;

    @Before
    public void setup() {
        provider = new TestSchemaProvider();
        mapper = new CMSTopStateMapper(provider);
        reducer = new CMSTopStateReducer();
        // mapper.setSchemaProvider(provider);
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void mapper_emits_values_by_reading_schema_from_hcatalog() throws Exception {
        // setup hcat schema to read for testing

        // TODO refactor me
        HCatSchema hCatSchema = HCatSchemaUtils.getListSchemaBuilder().build();
        hCatSchema.append(new HCatFieldSchema("general_transaction_id", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("program_year", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("payment_publication_date", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("submitting_applicable_manufacturer_or_applicable_gpo_name", TypeInfoFactory.stringTypeInfo,
                                              ""));
        hCatSchema.append(new HCatFieldSchema("covered_recipient_type", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("teaching_hospital_id", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("teaching_hospital_name", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("physician_profile_id", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("physician_first_name", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("physician_middle_name", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("physician_last_name", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("physician_name_suffix", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("recipient_primary_business_street_address_line1", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("recipient_primary_business_street_address_line2", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("recipient_city", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("recipient_state", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("recipient_zip_code", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("recipient_country", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("recipient_province", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("recipient_postal_code", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("physician_primary_type", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("physician_specialty", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("physician_license_state_code1", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("physician_license_state_code2", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("physician_license_state_code3", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("physician_license_state_code4", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("physician_license_state_code5", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("product_indicator", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("name_of_associated_covered_drug_or_biological1", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("name_of_associated_covered_drug_or_biological2", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("name_of_associated_covered_drug_or_biological3", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("name_of_associated_covered_drug_or_biological4", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("name_of_associated_covered_drug_or_biological5", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("ndc_of_associated_covered_drug_or_biological1", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("ndc_of_associated_covered_drug_or_biological2", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("ndc_of_associated_covered_drug_or_biological3", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("ndc_of_associated_covered_drug_or_biological4", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("ndc_of_associated_covered_drug_or_biological5", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("name_of_associated_covered_device_or_medical_supply1", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("name_of_associated_covered_device_or_medical_supply2", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("name_of_associated_covered_device_or_medical_supply3", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("name_of_associated_covered_device_or_medical_supply4", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("name_of_associated_covered_device_or_medical_supply5", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("applicable_manufacturer_or_applicable_gpo_making_payment_name",
                                              TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("applicable_manufacturer_or_applicable_gpo_making_payment_id",
                                              TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("applicable_manufacturer_or_applicable_gpo_making_payment_state",
                                              TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("applicable_manufacturer_or_applicable_gpo_making_payment_country",
                                              TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("dispute_status_for_publication", TypeInfoFactory.stringTypeInfo, ""));
        // hCatSchema.append(new HCatFieldSchema("total_amount_of_payment_usdollars", TypeInfoFactory.decimalTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("total_amount_of_payment_usdollars", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("date_of_payment", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("number_of_payments_included_in_total_amount", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("form_of_payment_or_transfer_of_value", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("nature_of_payment_or_transfer_of_value", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("city_of_travel", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("state_of_travel", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("country_of_travel", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("physician_ownership_indicator", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("third_party_payment_recipient_indicator", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("name_of_third_party_entity_receiving_payment_or_transfer_of_value",
                                              TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("charity_indicator", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("third_party_equals_covered_recipient_indicator", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("contextual_information", TypeInfoFactory.stringTypeInfo, ""));
        hCatSchema.append(new HCatFieldSchema("delay_in_publication_of_general_payment_indicator", TypeInfoFactory.stringTypeInfo, ""));
        // hCatSchema.append(new HCatFieldSchema("recipient_city", TypeInfoFactory.stringTypeInfo, ""));
        // hCatSchema.append(new HCatFieldSchema("recipient_state", TypeInfoFactory.stringTypeInfo, ""));
        // hCatSchema.append(new HCatFieldSchema("applicable_manufacturer_or_applicable_gpo_making_payment_name", TypeInfoFactory.stringTypeInfo, ""));
        // hCatSchema.append(new HCatFieldSchema("applicable_manufacturer_or_applicable_gpo_making_payment_id", TypeInfoFactory.stringTypeInfo, ""));
        // hCatSchema.append(new HCatFieldSchema("applicable_manufacturer_or_applicable_gpo_making_payment_state", TypeInfoFactory.stringTypeInfo, ""));
        // hCatSchema.append(new HCatFieldSchema("applicable_manufacturer_or_applicable_gpo_making_payment_country", TypeInfoFactory.stringTypeInfo, ""));
        // hCatSchema.append(new HCatFieldSchema("total_amount_of_payment_usdollars", TypeInfoFactory.stringTypeInfo, ""));

        provider.setSchema(hCatSchema);

        // setup hcatrecords
        List<DefaultHCatRecord> hCatRecords = new ArrayList<DefaultHCatRecord>();
        for (String[] rec : readFile("src/test/resources/sample.txt")) {
            System.out.println(rec[15] + "," + rec[48]);
            DefaultHCatRecord hcr = new DefaultHCatRecord(rec.length);
            for (int i = 0; i < rec.length; i++) {
                hcr.set(i, rec[i]);
            }
            hCatRecords.add(hcr);
            mapDriver.withInput(new LongWritable(), hcr);
        }

        // mapDriver.withAll(hCatRecords);
        mapDriver.withOutput(new Text("CA"), new Text("69"));
        mapDriver.withOutput(new Text("TX"), new Text("14.38"));
        mapDriver.withOutput(new Text("MI"), new Text("13.53"));
        mapDriver.withOutput(new Text("CA"), new Text("18.3"));
        mapDriver.withOutput(new Text("IL"), new Text("16.06"));
        mapDriver.runTest();
    }

    @Test
    public void reducer_totals_payments_by_state() throws Exception {
        reduceDriver.withInput(new Text("CA"), Arrays.asList(new Text[]{ new Text("69"), new Text("18.3")}));
        reduceDriver.withInput(new Text("TX"), Arrays.asList(new Text[]{ new Text("14.38") }));
        reduceDriver.withInput(new Text("MI"), Arrays.asList(new Text[]{ new Text("13.53")}));
        reduceDriver.withInput(new Text("IL"), Arrays.asList(new Text[]{ new Text("16.06")}));
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

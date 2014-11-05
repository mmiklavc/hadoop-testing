package com.michaelmiklavcic;

import java.io.*;

import org.apache.pig.pigunit.PigTest;
import org.junit.*;

import com.google.common.io.Files;

public class LoaderTest {

    private File tmpDir;

    @Before
    public void setup() {
        tmpDir = Files.createTempDir();
    }

    @Test
    public void loads_data_normal_with_delimiter_counting_and_NO_line_breaks() throws Exception {
        String[] input = {
                "a1\ta2-somea\ta3",
                "b1\tb2-someb\tb3",
                "c1\tc2-somec\tc3" };
        String inputFileName = tmpDir.getAbsolutePath() + "/2014-data.txt";
        writeFile(inputFileName, input);
        String[] script = {
                "data = LOAD '" + inputFileName + "' using com.michaelmiklavcic.DatestampLoader('yyyy') as (col1, col2, col3, insert_ts);",
                "store data into 'someoutdata';" };
        String[] output = {
                "(a1,a2-somea,a3,2014)",
                "(b1,b2-someb,b3,2014)",
                "(c1,c2-somec,c3,2014)", };
        PigTest test = new PigTest(script);
        test.assertOutput("data", output);
    }

    private void writeFile(String fileName, String[] input) throws Exception {
        File fileOut = new File(fileName);
        FileWriter writer = new FileWriter(fileOut);
        for (String line : input) {
            writer.write(line + "\n");
        }
        writer.close();
    }

}

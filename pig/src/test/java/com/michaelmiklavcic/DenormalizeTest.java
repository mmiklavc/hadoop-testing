package com.michaelmiklavcic;

import java.io.*;
import java.nio.file.*;

import org.apache.pig.pigunit.PigTest;
import org.junit.*;

public class DenormalizeTest {

    private Path tempDirectory;

    @Before
    public void setUp() throws Exception {
        tempDirectory = Files.createTempDirectory(null);
    }

    @Test
    public void denormalize_data_outputs_flattened() throws Exception {
        String[] stateIn = {
                "1\tOH",
                "2\tCA",
                "3\tNY" };
        String[] personIn = {
                "1\t1\tMichae Miklavcic",
                "2\t1\tMr Joe",
                "3\t2\tRob Lowe",
                "4\t2\tBruce Campbell",
                "5\t3\tConan O'Brian" };
        String[] expectedOut = {
                "(2,1,Mr Joe,1,OH)",
                "(1,1,Michae Miklavcic,1,OH)",
                "(4,2,Bruce Campbell,2,CA)",
                "(3,2,Rob Lowe,2,CA)",
                "(5,3,Conan O'Brian,3,NY)" };
        String[] script = {
                "STATE = LOAD people.state using HCatLoader();",
                "PERSON = LOAD people.persons using HCatLoader();",
                "DENORM = JOIN PERSON BY state_id LEFT, STATE BY state_id;",
                "STORE DENORM INTO 'out';" };
        File statePath = writeFile(stateIn, "states.txt");
        File personPath = writeFile(personIn, "persons.txt");
        File scriptPath = writeFile(script, "pigscript.pig");

        PigTest pig = new PigTest(scriptPath.getAbsolutePath());
        pig.override("STATE", "STATE = LOAD '" + statePath.getAbsolutePath() + "' as (state_id:chararray, state_name:chararray);");
        pig.override("PERSON", "PERSON = LOAD '" + personPath.getAbsolutePath()
                + "' as (person_id:chararray,state_id:chararray, person_name:chararray);");
        pig.assertOutput("DENORM", expectedOut);
    }

    private File writeFile(String[] contents, String name) throws IOException {
        File out = new File(tempDirectory.toFile(), name);
        try (PrintWriter w = new PrintWriter(new FileWriter(out))) {
            for (String line : contents) {
                w.println(line);
            }
        }
        return out;
    }

}

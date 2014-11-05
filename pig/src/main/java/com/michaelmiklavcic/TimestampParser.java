package com.michaelmiklavcic;

import java.text.*;
import java.util.Date;

public class TimestampParser {

    private SimpleDateFormat format;

    public TimestampParser(String format) {
        this.format = new SimpleDateFormat(format);
    }

    public String parseFrom(String input) {
        try {
            Date date = format.parse(input);
            return format.format(date);
        } catch (ParseException e) {
            throw new RuntimeException("Could not parse date from file");
        }
    }

}

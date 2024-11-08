package com.zendesk.maxwell.schema.columndef;

public class DateValidator {
    private static final String DATE_TIME_REGEX = 
    "^\\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])" +
    "( (0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9]))?$";

    public static boolean isValidDateTime(String dateString) {
        return dateString.matches(DATE_TIME_REGEX);
    }
}

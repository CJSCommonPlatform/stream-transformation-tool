package uk.gov.justice.tools.eventsourcing.anonymization.constants;


import static java.util.regex.Pattern.compile;

import java.util.regex.Pattern;

public class StringPattern {

    public static final Pattern UUID_PATTERN = compile("^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$");
    public static final Pattern DATE_PATTERN = compile("^((([\\+-]?\\d{4}(?!\\d{2}\\b))\\-(0[13578]|1[02])\\-(0[1-9]|[12]\\d|3[01]))|(([\\+-]?\\d{4}(?!\\d{2}\\b))\\-(0[13456789]|1[012])\\-(0[1-9]|[12]\\d|30))|(([\\+-]?\\d{4}(?!\\d{2}\\b))\\-02\\-(0[1-9]|1\\d|2[0-8]))|(((1[6-9]|[2-9]\\d)(0[48]|[2468][048]|[13579][26])|((16|[2468][048]|[3579][26])00))\\-02\\-29))$");
    public static final Pattern EMAIL_PATTERN = compile("^[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?\\.)+[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?$");
    public static final Pattern DATE_TIME_PATTERN = compile("^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])(T|\\s)(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(.[0-9]+)?(Z)?$");
    public static final Pattern NI_NUMBER_PATTERN = compile("(?!BG)(?!GB)(?!NK)(?!KN)(?!TN)(?!NT)(?!ZZ)(?:[A-CEGHJ-PR-TW-Z][A-CEGHJ-NPR-TW-Z])(?:\\s*\\d\\s*){6}([A-D]|\\s)$");
    public static final Pattern POSTCODE_PATTERN = compile("^(([gG][iI][rR] {0,}0[aA]{2})|(([aA][sS][cC][nN]|[sS][tT][hH][lL]|[tT][dD][cC][uU]|[bB][bB][nN][dD]|[bB][iI][qQ][qQ]|[fF][iI][qQ][qQ]|[pP][cC][rR][nN]|[sS][iI][qQ][qQ]|[iT][kK][cC][aA]) {0,}1[zZ]{2})|((([a-pr-uwyzA-PR-UWYZ][a-hk-yxA-HK-XY]?[0-9][0-9]?)|(([a-pr-uwyzA-PR-UWYZ][0-9][a-hjkstuwA-HJKSTUW])|([a-pr-uwyzA-PR-UWYZ][a-hk-yA-HK-Y][0-9][abehmnprv-yABEHMNPRV-Y]))) {0,}[0-9][abd-hjlnp-uw-zABD-HJLNP-UW-Z]{2}))$");
    public static final Pattern PHONE_NUMBER_PATTERN = compile("^[\\d]*$");

    private StringPattern() {
        // should not create an instance of this class
    }
}

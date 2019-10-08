package util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public final class TimeUtil {
    /**
     * @param dateStr like "2019-10-06 09:41"
     * @return java.util.Date
     */
    public static Date parseDate(String dateStr) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        Date date = null;
        try {
            date = dateFormat.parse(dateStr);
        } catch (ParseException e) {
            System.err.println("Failed to parse dateStr: " + dateStr);
        }
        return date;
    }

    /**
     * judge if target during period [startYear, endYear)
     *
     * @param target    target date
     * @param startYear included
     * @param endYear   excluded
     * @return true or false
     */
    public static boolean isBetween(Date target, int startYear, int endYear) {
        Date startDate = parseDate(startYear + "-01-01 00:00:00");
        Date endDate = parseDate(endYear + "-01-01 00:00:00");
        return target.before(endDate) && startDate.before(target);
    }

    public static void main(String[] args) {
        Date date = parseDate("2019-10-06 09:41");
        System.out.println(date.toString());
    }
}

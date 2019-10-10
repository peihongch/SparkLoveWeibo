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

    public static String extractYerAndMonth(String dateStr) {
        String[] splits = dateStr.split("-");
        int month = Integer.parseInt(splits[1]);
        if (month < 4) {
            return splits[0] + "_" + 1;
        } else if (month < 7) {
            return splits[0] + "_" + 4;
        } else if (month < 10) {
            return splits[0] + "_" + 7;
        } else return splits[0] + "_" + 10;
    }

    /**
     * judge if target during period [startYear, endYear)
     *
     * @param target    target date
     * @param startYear included
     * @param endYear   excluded
     * @param month     start month
     * @return true or false
     */
    public static boolean isBetween(Date target, int startYear, int endYear, int month) {
        String startMonStr, endMonStr, startYearStr = startYear + "", endYearStr;
        if (month < 10) {
            startMonStr = "0" + month;
            if (month < 7) endMonStr = "0" + (month + 4);
            else endMonStr = "" + (month + 4);
            endYearStr = startYear + "";
        } else {
            startMonStr = "" + month;
            endMonStr = "01";
            endYearStr = endYear + "";
        }
        Date startDate = parseDate(startYearStr + "-" + startMonStr + "-01 00:00:00");
        Date endDate = parseDate(endYearStr + "-" + endMonStr + "-01 00:00:00");
        return target.before(endDate) && startDate.before(target);
    }

    public static void main(String[] args) {
        System.out.println(extractYerAndMonth("2018-02-06 09:41"));
        final String template = "{\"%s\":{\"id\":\"%s\",\"forward\":\"%s\",\"at\":\"%s\",\"likeNum\":\"%s,\"forwardNum\":\"%s,\"commentNum\":\"%s\"}}";
        System.out.println(String.format(template, 1,1,1,1,1,1,1));
    }
}

package org.jgroups;

import java.sql.Time;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public class Main {

   public static void main(String[] args) {
      //1578574463.203682000
      long nano = 1578574463;
      long milli = nano * 1000;
      Date expiry = new Date(milli);
      ZoneId gmt = ZoneId.of("GMT-03:00");
      System.out.println(expiry);
      System.out.println(Instant.ofEpochMilli(milli).atZone(gmt));

      /*
      System.out.println(expiry);
      System.out.println(Instant.now().getEpochSecond());
      System.out.println(ZonedDateTime.ofInstant(Instant.ofEpochMilli(nano * 1000), ZoneId.systemDefault()));
      System.out.println(Instant.ofEpochMilli(nano * 1000));
      System.out.println(Instant.ofEpochMilli(nano * 1000).atZone(ZoneId.systemDefault()));
      System.out.println(ZoneId.systemDefault().getId());
      System.out.println(ZoneId.getAvailableZoneIds());

       */
   }
}

package org.jgroups.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.jgroups.tests.ParseMessages;

/**
 * This an utility class that parse one or more TShark fields
 * It handle files generated with tshark -q -i eth2 -Tfields -e data -e frame.time_epoch udp and dst 234.99.54.20 > foo.txt
 *
 * This class doesn't support stream file
 *
 * @author Diego Lovison &lt;dlovison@redhat.com&gt;
 */
public class TSharkDumpUtil {

   private static final DateTimeFormatter TIME_EPOCH_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss,SSS");

   // tshark support fields
   private static final String FIELD_DATA = "data";
   private static final String FIELD_TIME_EPOCH = "frame.time_epoch";

   public static void main(String[] args) {

      String file=null;
      String[] tSharkFields=new String[] {"data"};
      ZoneId zoneId=ZoneId.systemDefault();

      for(int i=0; i < args.length; i++) {
         if (args[i].equals("-file")) {
            file = args[++i];
            continue;
         }
         if("-tshark-fields".equalsIgnoreCase(args[i])) {
            tSharkFields=args[++i].split(",");
            continue;
         }
         if ("-gmt".equals(args[i])) {
            zoneId=ZoneId.of(args[++i]);
            continue;
         }
      }

      boolean parse = true;
      boolean print_version = true;
      AtomicInteger cnt = new AtomicInteger(1);

      // we would like to see the date as the first argument in the printed line
      List<String> messages = Collections.synchronizedList(new ArrayList<>()); ;
      ParseMessages.MessageConsumer messageConsumer = new ParseMessages.MessageConsumer(parse, print_version, cnt) {
         @Override
         public void print(String message) {
            messages.add(message);
         }
         @Override
         public void printErr(String message) {
            messages.add(message);
         }
      };
      ParseMessages.BatchConsumer batchConsumer = new ParseMessages.BatchConsumer(parse, print_version, cnt) {
         @Override
         public void print(String message) {
            messages.add(message);
         }
         @Override
         public void printErr(String message) {
            messages.add(message);
         }
      };

      try (BufferedReader reader = new BufferedReader(new FileReader(file))){
         String line = reader.readLine();
         while (line != null) {
            if (!(line.contains("packets dropped") || line.contains("packets captured"))) {
               String[] data = line.split("\t");
               String epochTime = null;
               for (int i = 0; i < tSharkFields.length; i++) {
                  String tSharkField = tSharkFields[i];
                  if (FIELD_DATA.equals(tSharkField)) {
                     try (InputStream result = new ByteArrayInputStream(data[i].getBytes(StandardCharsets.UTF_8))) {
                        Util.parse(new ParseMessages.BinaryToAsciiInputStream(result), messageConsumer, batchConsumer, false, true);
                     } catch (Exception e) {
                        messages.add(e.getCause().getMessage());
                     }
                  } else if (FIELD_TIME_EPOCH.equals(tSharkField)){
                     String[] time = data[i].split("\\.");
                     long epochMilli = Long.valueOf(time[0]) * 1000;
                     long nanos = Long.valueOf(time[1]);
                     ZonedDateTime zonedDateTime = Instant.ofEpochMilli(epochMilli).plusNanos(nanos).atZone(zoneId);
                     epochTime = TIME_EPOCH_DATE_TIME_FORMATTER.format(zonedDateTime);
                  } else {
                     throw new UnsupportedOperationException(String.format("%s it not a valid tshark field reader", tSharkField));
                  }
               }
               for (String m : messages) {
                  if (epochTime != null) {
                     System.out.print(String.format("%s %s", epochTime, m));
                  } else {
                     System.out.print(m);
                  }
               }

               messages.clear();
            }
            line = reader.readLine();
         }
      } catch (IOException e) {
         e.printStackTrace();
      }
   }
}

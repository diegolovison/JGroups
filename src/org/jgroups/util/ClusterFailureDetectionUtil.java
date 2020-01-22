package org.jgroups.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.protocols.FD_ALL;
import org.jgroups.protocols.PingHeader;
import org.jgroups.tests.ParseMessages;

public class ClusterFailureDetectionUtil {

   // tshark support fields
   private static final String FIELD_DATA = "data";
   private static final String FIELD_TIME_EPOCH = "frame.time_epoch";

   public static void main(String[] args) throws IOException {
      String file=null;
      String[] tSharkFields=new String[] {"data"};
      ZoneId zoneId=ZoneId.systemDefault();
      String jgroupsLogFolder=null;

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
         if ("-jgroupsLogFolder".equals(args[i])) {
            jgroupsLogFolder=args[++i];
            continue;
         }
      }

      ClusterFailureDetectionUtil fooBarDetection = new ClusterFailureDetectionUtil();
      fooBarDetection.printTShark(file, tSharkFields, zoneId);
      if (jgroupsLogFolder != null) {
         fooBarDetection.foo(jgroupsLogFolder);
      }
   }

   public void foo(String jgroupsLogFolder) throws IOException {

      String fileNameRegex = "server-\\d.log";

      File folder = new File(jgroupsLogFolder);
      String[] files = folder.list();

      for (String fileName : files) {
         if (fileName.matches(fileNameRegex)) {
            File logFile = new File(jgroupsLogFolder, fileName);
            Files.readAllLines(Paths.get(logFile.getAbsolutePath())).stream().forEach(line -> {
               if (line.contains("sent heartbeat")) {
                  System.out.println(line);
               }
            });
         }
      }
   }

   public void printTShark(String file, String[] tSharkFields, ZoneId zoneId) {


      boolean parse = true;
      boolean print_version = true;
      AtomicInteger cnt = new AtomicInteger(1);

      class EpochMessage {
         Message message;
         ZonedDateTime zonedDateTime;

         @Override
         public String toString() {
            StringBuilder sb = new StringBuilder();
            if (message.getHeaders() != null) {

            }
            return "EpochMessage{" +
                  "message=" + message +
                  '}';
         }
      }

      List<EpochMessage> all = new ArrayList<>();
      List<EpochMessage> remaining = new ArrayList<>();

      ParseMessages.MessageConsumer messageConsumer = new ParseMessages.MessageConsumer(parse, print_version, cnt) {
         @Override
         protected void acceptMessage(Message msg, Short version) {
            EpochMessage epochMessage = new EpochMessage();
            epochMessage.message = msg;
            remaining.add(epochMessage);
         }
         @Override
         public void print(String message) {}
         @Override
         public void printErr(String message) {}
      };
      ParseMessages.BatchConsumer batchConsumer = new ParseMessages.BatchConsumer(parse, print_version, cnt) {
         @Override
         protected void acceptMessage(Message msg, int index, Short version) {
            EpochMessage epochMessage = new EpochMessage();
            epochMessage.message = msg;
            remaining.add(epochMessage);
         }

         @Override
         public void print(String message) {}
         @Override
         public void printErr(String message) {}
      };

      try (BufferedReader reader = new BufferedReader(new FileReader(file))){
         String line = reader.readLine();
         while (line != null) {
            if (!(line.contains("packets dropped") || line.contains("packets captured"))) {
               String[] data = line.split("\t");
               ZonedDateTime epochTime = null;
               for (int i = 0; i < tSharkFields.length; i++) {
                  String tSharkField = tSharkFields[i];
                  if (FIELD_DATA.equals(tSharkField)) {
                     try (InputStream result = new ByteArrayInputStream(data[i].getBytes(StandardCharsets.UTF_8))) {
                        try {
                           Util.parse(new ParseMessages.BinaryToAsciiInputStream(result), messageConsumer, batchConsumer, false, true);
                        } catch (Exception e) {
                           System.out.println(line + " -> " + e.getMessage());
                        }
                     }
                  } else if (FIELD_TIME_EPOCH.equals(tSharkField)){
                     String[] time = data[i].split("\\.");
                     long epochMilli = Long.valueOf(time[0]) * 1000;
                     long nanos = Long.valueOf(time[1]);
                     epochTime = Instant.ofEpochMilli(epochMilli).plusNanos(nanos).atZone(zoneId);
                  } else {
                     throw new UnsupportedOperationException(String.format("%s it not a valid tshark field reader", tSharkField));
                  }
               }

               for (EpochMessage message : remaining) {
                  message.zonedDateTime = epochTime;
                  all.add(message);
               }
               remaining.clear();
            }
            line = reader.readLine();
         }
      } catch (IOException e) {
         e.printStackTrace();
      }

      class HeartbeatHeaderData {
         int uuid;
         ZonedDateTime epochTime;
      }

      Map<String, List<HeartbeatHeaderData>> map = new HashMap<>();

      for (EpochMessage m : all) {
         Map<Short,Header>  headers = m.message.getHeaders();
         for (Short key : headers.keySet()) {
            Header header = headers.get(key);
            if (header instanceof FD_ALL.HeartbeatHeader) {
               FD_ALL.HeartbeatHeader fdAllHeader = (FD_ALL.HeartbeatHeader) header;
               String sender = m.message.getSrc().toString();
               int uuid = fdAllHeader.getUuid();
               List<HeartbeatHeaderData> heartbeatHeaderDataList = map.get(sender);
               if (heartbeatHeaderDataList == null) {
                  heartbeatHeaderDataList = new ArrayList<>();
                  map.put(sender, heartbeatHeaderDataList);
               }

               HeartbeatHeaderData data = new HeartbeatHeaderData();
               data.uuid = uuid;
               data.epochTime = m.zonedDateTime;
               heartbeatHeaderDataList.add(data);
            } else if (header instanceof PingHeader) {
               System.out.println(header);
            }

         }
      }

      long maxDiff = 10;
      for (String host : map.keySet()) {
         List<HeartbeatHeaderData> hostHeartBeatList = map.get(host);
         long last = 0;
         for (HeartbeatHeaderData headerData : hostHeartBeatList) {
            long current = headerData.epochTime.toEpochSecond();
            if (last > 0) {
               if (current - last > maxDiff) {
                  System.out.println(String.format("sender: %s, uuid: %d", host, headerData.uuid));
               }
            }
            last = headerData.epochTime.toEpochSecond();
         }
      }
   }
}

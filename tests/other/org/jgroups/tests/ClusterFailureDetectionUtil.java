package org.jgroups.tests;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.border.MatteBorder;

import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.protocols.FD_ALL;
import org.jgroups.protocols.PingHeader;
import org.jgroups.util.Util;

public class ClusterFailureDetectionUtil {

   private static final DateTimeFormatter TIME_EPOCH_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS");

   private static final int diffHours = 3;

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

      List<File> serverLogs = fooBarDetection.getServerLogs(jgroupsLogFolder);
      Map<String, String> mappings = fooBarDetection.getMapping(serverLogs);
      Map<String, List<HeartbeatHeaderData>> tsharkDump = fooBarDetection.get_FD_ALL_TShark(file, tSharkFields, zoneId, mappings);
      Map<String, List<HeartbeatHeaderData>> jgroupsLogsDump = fooBarDetection.read_FD_ALL_fromJGroupsLog(serverLogs, zoneId);

      class Diff {
         LocalTime tsharkTime;
         LocalTime jgroupsTime;
         long diffMs;
         String sender;
         int uuid;
      }

      Map<String, List<Diff>> diffMap = new HashMap<>();

      // tshark can start later and has less values when compared with logs
      for (String sender : tsharkDump.keySet()) {
         List<HeartbeatHeaderData> tsharkDataList = tsharkDump.get(sender);
         List<HeartbeatHeaderData> logDataList = jgroupsLogsDump.get(sender);

         for (HeartbeatHeaderData tsharkData : tsharkDataList) {

            int logDataIndex = logDataList.indexOf(tsharkData);
            HeartbeatHeaderData logData = logDataList.get(logDataIndex);

            long tsharkTime = tsharkData.epochTime.toInstant().toEpochMilli();
            long jGroupsTime = logData.epochTime.toInstant().toEpochMilli();
            long diffMs = Math.abs(tsharkTime - jGroupsTime);

            if (diffMs > 1_000) {
               Diff d = new Diff();
               d.tsharkTime = tsharkData.epochTime.toLocalTime().minusHours(diffHours);
               d.jgroupsTime = logData.epochTime.toLocalTime().minusHours(diffHours);
               d.diffMs = diffMs;
               d.sender = sender;
               d.uuid = tsharkData.uuid;

               List<Diff> diffs = diffMap.get(sender);
               if (diffs == null) {
                  diffs = new ArrayList<>();
                  diffMap.put(sender, diffs);
               }
               diffs.add(d);
            }
         }
      }

      System.out.println(diffMap);
   }

   public List<File> getServerLogs(String jgroupsLogFolder) {
      List<File> serverLogs = new ArrayList<>();
      String fileNameRegex = "server-\\d.log";

      File folder = new File(jgroupsLogFolder);
      String[] files = folder.list();

      for (String fileName : files) {
         if (fileName.matches(fileNameRegex)) {
            File logFile = new File(jgroupsLogFolder, fileName);
            serverLogs.add(logFile);
         }
      }
      return serverLogs;
   }

   public Map<String, String> getMapping(List<File> serverLogs) throws IOException {
      Pattern pattern = Pattern.compile("(.*)local_addr: (.*), name: (.*)");
      Map<String, String> mapping = new HashMap<>();
      for (File logFile : serverLogs) {
         BufferedReader buffReader = new BufferedReader(new InputStreamReader(new FileInputStream(logFile)));
         String line = buffReader.readLine();
         while (line != null) {
            if (line.contains("TRACE")) {
               Matcher matcher = pattern.matcher(line);
               if (matcher.find()) {
                  String uuid = matcher.group(2);
                  String name = matcher.group(3);
                  mapping.put(uuid, name);
                  break;
               }
            }
            line = buffReader.readLine();
         }
      }
      if (serverLogs.size() != mapping.size()) {
         throw new IllegalStateException("Mapping must match");
      }
      return mapping;
   }

   public Map<String, List<HeartbeatHeaderData>> read_FD_ALL_fromJGroupsLog(List<File> serverLogs, ZoneId zoneId) throws IOException {
      Map<String, List<HeartbeatHeaderData>> map = new HashMap<>();
      Pattern patternFD_ALL = Pattern.compile("\\[FD_ALL\\] (.*): sent heartbeat\\((.*)\\)");
      Pattern patternEpoch = Pattern.compile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}");

      for (File logFile : serverLogs) {
         Files.readAllLines(Paths.get(logFile.getAbsolutePath())).stream().forEach(line -> {
            if (line.contains("sent heartbeat")) {
               Matcher matcherFD_ALL = patternFD_ALL.matcher(line);
               if (matcherFD_ALL.find()) {

                  // data
                  String sender = matcherFD_ALL.group(1);
                  int uuid = Integer.valueOf(matcherFD_ALL.group(2));
                  List<HeartbeatHeaderData> list = map.get(sender);
                  if (list == null) {
                     list = new ArrayList<>();
                     map.put(sender, list);
                  }

                  // epoch
                  ZonedDateTime epochTime;
                  Matcher matcherEpoch = patternEpoch.matcher(line);
                  if (matcherEpoch.find()) {
                     String date = matcherEpoch.group();
                     LocalDateTime localDateTime = LocalDateTime.parse(date, TIME_EPOCH_DATE_TIME_FORMATTER).plusHours(diffHours);
                     epochTime = ZonedDateTime.of(localDateTime, zoneId);
                  } else {
                     throw new IllegalStateException("It must have the date");
                  }

                  HeartbeatHeaderData data = new HeartbeatHeaderData();
                  data.uuid = uuid;
                  data.epochTime = epochTime;
                  list.add(data);
               }
            }
         });
      }
      return map;
   }

   public Map<String, List<HeartbeatHeaderData>> get_FD_ALL_TShark(String file, String[] tSharkFields, ZoneId zoneId, Map<String, String> mappings) {

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

      Map<String, List<HeartbeatHeaderData>> map = new HashMap<>();

      for (EpochMessage m : all) {
         Map<Short,Header>  headers = m.message.getHeaders();
         for (Short key : headers.keySet()) {
            Header header = headers.get(key);
            if (header instanceof FD_ALL.HeartbeatHeader) {
               FD_ALL.HeartbeatHeader fdAllHeader = (FD_ALL.HeartbeatHeader) header;
               String sender = mappings.get(m.message.getSrc().toString());
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
            }

         }
      }
      return map;
   }

   private static class HeartbeatHeaderData {
      int uuid;
      ZonedDateTime epochTime;

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         HeartbeatHeaderData that = (HeartbeatHeaderData) o;
         return uuid == that.uuid;
      }

      @Override
      public int hashCode() {
         return Objects.hash(uuid);
      }
   }
}

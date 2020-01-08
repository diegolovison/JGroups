package org.jgroups.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//import com.google.gson.Gson;

public class ParseHeartbeatHeader {

   public static void main(String[] args) throws IOException {
      // path to the log files
      String path = args[0];
      // server-\d.log
      String fileNameRegex = args[1];
      // ms to not add in the list ( we really need add to the list messages that took 5 ms ? )
      long elapsedFilter = Long.valueOf(args[2]);

      File folder = new File(path);
      String[] files = folder.list();

      Map<String, List<String>> sentHeartbeat = new HashMap<>();
      Map<String, List<String>> receivedHeartbeat = new HashMap<>();
      Map<String, List<String>> elapsed = new HashMap<>();
      Map<String, Map<String, Long>> sentHeartbeatLocal = new HashMap<>();
      Map<String, Map<String, Long>> receivedHeartbeatLocal = new HashMap<>();
      Map<String, List<HeartbeatSenderLog>> heartbeatSenderLog = new HashMap<>();
      for (String fileName : files) {
         if (fileName.matches(fileNameRegex)) {
            File logFile = new File(path, fileName);
            List<String> sentHeartbeatList = new ArrayList<>();
            List<String> receivedHeartbeatList = new ArrayList<>();
            List<String> elapsedList = new ArrayList<>();
            Map<String, Long> sentHeartbeatLocalMap = new HashMap<>();
            Map<String, Long> receivedHeartbeatLocalMap = new HashMap<>();
            List<HeartbeatSenderLog> heartbeatSenderLogList = new ArrayList<>();
            Files.readAllLines(Paths.get(logFile.getAbsolutePath())).stream().forEach(line -> {
               if (line.contains("sent heartbeat")) {

                  // id
                  String id = getHearbeatId(line);
                  sentHeartbeatList.add(id);

                  // local diff map
                  sentHeartbeatLocalMap.put(id, getLogEntryTime(line));

               } else if (line.contains("received heartbeat") && line.contains("FD_ALL::up::hdr")) {

                  // id
                  String id = getHearbeatId(line);
                  receivedHeartbeatList.add(id);

                  // elapsed
                  Long elapsedMs = getElapsedMs(line, "\\d+ms");
                  if (elapsedMs > elapsedFilter) {
                     elapsedList.add(String.valueOf(elapsedMs));
                  }

                  // local diff map
                  receivedHeartbeatLocalMap.put(id, getLogEntryTime(line));
               } else if (line.contains("HeartbeatSender")) {
                  Long elapsedMs = getElapsedMs(line, "\\d+ms");

                  HeartbeatSenderLog log = new HeartbeatSenderLog();
                  log.when = getLogEntryTime(line);
                  log.elapsed = elapsedMs;

                  heartbeatSenderLogList.add(log);
               }
            });
            sentHeartbeat.put(fileName, sentHeartbeatList);
            receivedHeartbeat.put(fileName, receivedHeartbeatList);
            elapsed.put(fileName, elapsedList);
            sentHeartbeatLocal.put(fileName, sentHeartbeatLocalMap);
            receivedHeartbeatLocal.put(fileName, receivedHeartbeatLocalMap);
            heartbeatSenderLog.put(fileName, heartbeatSenderLogList);
         }
      }

      printDelayReceivedMessages(sentHeartbeatLocal, receivedHeartbeatLocal);
      printDelaySentMessages(heartbeatSenderLog);
   }

   private static Long getElapsedMs(String line, String regex) {
      String elapsedMs = null;
      Matcher m = Pattern.compile(regex).matcher(line);
      if (m.find()) {
         elapsedMs = m.group(0);
      }
      if (elapsedMs == null) {
         throw new NullPointerException();
      }
      elapsedMs = elapsedMs.replaceAll("ms", "");
      return Long.valueOf(elapsedMs);
   }

   private static class HeartbeatSenderLog {
      private Long when;
      private Long elapsed;
   }

   private static void printDelaySentMessages(Map<String, List<HeartbeatSenderLog>> heartbeatSenderLog) {
      List<String> diffInHost = new ArrayList<>();
      for (String host : heartbeatSenderLog.keySet()) {
         List<HeartbeatSenderLog> list = heartbeatSenderLog.get(host);
         Long previous = null;
         for (HeartbeatSenderLog log : list) {
            if (previous != null) {
               long diff = log.when - previous;
               if (diff > 3_000) {
                  diffInHost.add(String.format("%s %s: diff previous %dms", createSimpleDateFormat().format(new Date(log.when)), host, diff));
               }
            }
            if (log.elapsed > 3_000) {
               diffInHost.add(String.format("%s %s: elapsed %dms", createSimpleDateFormat().format(new Date(log.when)), host, log.elapsed));
            }
            previous = log.when;
         }
      }

      System.out.println(diffInHost);
   }

   private static void printDelayReceivedMessages(Map<String, Map<String, Long>> sentHeartbeatLocal, Map<String, Map<String, Long>> receivedHeartbeatLocal) {

      Map<String, Map<String, Map<String, String>>> sentHeartbeatLocalDiff = new HashMap<>();
      for (String host : sentHeartbeatLocal.keySet()) {
         Map<String, Long> sent = sentHeartbeatLocal.get(host);
         Map<String, Map<String, String>> receivedHostDiff = new HashMap<>();

         for (String receivedHost : receivedHeartbeatLocal.keySet()) {

            Map<String, String> uuidMap = new HashMap<>();
            Map<String, Long> received = receivedHeartbeatLocal.get(receivedHost);

            for (String id : sent.keySet()) {
               Long begin = sent.get(id);
               Long end = received.get(id);
               if (end != null) {
                  Long diff = end - begin;
                  if (diff > 1_000) {
                     uuidMap.put(id, String.valueOf(diff));
                  }
               } else {
                  uuidMap.put(id, "<not_received>");
               }
            }
            receivedHostDiff.put(receivedHost, uuidMap);
         }
         sentHeartbeatLocalDiff.put(host, receivedHostDiff);
      }

      //System.out.println(new Gson().toJson(sentHeartbeatLocalDiff));
   }

   private static long getLogEntryTime(String line) {
      line += "2020-01-08 " + line;
      Date d = null;
      Matcher m = Pattern.compile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}").matcher(line);
      if (m.find()) {
         try {
            d = createSimpleDateFormat().parse(m.group());
         } catch (ParseException e) {
            throw new IllegalStateException(e);
         }
      }
      return d.getTime();
   }

   private static SimpleDateFormat createSimpleDateFormat() {
      return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
   }

   private static String getHearbeatId(String line) {
      Matcher m = Pattern.compile("\\((.*?)\\)").matcher(line);
      List<String> matchList = new ArrayList<>();
      while (m.find()) {
         matchList.add(m.group(1));
      }
      String id = matchList.get(matchList.size() - 1);
      return id;
   }

   private static void keepMissing(Map<String, List<String>> sentHeartbeat, Map<String, List<String>> receivedHeartbeat) {
      Map<String, Map<String, String>> missing = new HashMap<>();
      for (String host : sentHeartbeat.keySet()) {
         List<String> sentHeartbeatList = sentHeartbeat.get(host);
         Map<String, String> map = new HashMap<>();
         sentHeartbeat.keySet().stream().filter(key -> !key.equals(host)).forEach(key -> {
            List<String> received = receivedHeartbeat.get(key);
            for (String sent : sentHeartbeatList) {
               if (!received.contains(sent)) {
                  map.put(key, sent);
               }
            }
            missing.put(host, map);
         });
      }
   }

   private static void keepWhatWasNotSent(Map<String, List<String>> sentHeartbeat, Map<String, List<String>> receivedHeartbeat) {
      for (String host : sentHeartbeat.keySet()) {
         List<String> sentHeartbeatList = sentHeartbeat.get(host);
         List<String> receivedHeartbeatList = receivedHeartbeat.get(host);

         // remove what was sent
         receivedHeartbeatList.removeAll(sentHeartbeatList);

         // remove others
         sentHeartbeat.keySet().parallelStream().filter(key -> !key.equals(host)).forEach(key -> receivedHeartbeat.get(key).removeAll(sentHeartbeatList));
      }
   }
}

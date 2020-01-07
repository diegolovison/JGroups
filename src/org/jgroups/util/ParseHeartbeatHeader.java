package org.jgroups.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParseHeartbeatHeader {

   public static void main(String[] args) throws IOException {
      // path to the log files
      String path = args[0];
      // server-\d.log
      String fileNameRegex = args[1];
      // ms to not add in the list ( we really need add to the list messages that took 5 ms ? )
      int elapsedFilter = Integer.valueOf(args[2]);

      File folder = new File(path);
      String[] files = folder.list();

      Map<String, List<String>> sentHeartbeat = new HashMap<>();
      Map<String, List<String>> receivedHeartbeat = new HashMap<>();
      Map<String, List<String>> elapsed = new HashMap<>();
      for (String fileName : files) {
         if (fileName.matches(fileNameRegex)) {
            File logFile = new File(path, fileName);
            List<String> sentHeartbeatList = new ArrayList<>();
            List<String> receivedHeartbeatList = new ArrayList<>();
            List<String> elapsedList = new ArrayList<>();
            Files.readAllLines(Paths.get(logFile.getAbsolutePath())).stream().forEach(line -> {
               if (line.contains("sent heartbeat")) {
                  Matcher m = Pattern.compile("\\((.*?)\\)").matcher(line);
                  List<String> matchList = new ArrayList<>();
                  while (m.find()) {
                     matchList.add(m.group(1));
                  }
                  String id = matchList.get(matchList.size() - 1);
                  if (sentHeartbeatList.contains(id)) {
                     throw new IllegalStateException("Why have the same id?");
                  }
                  sentHeartbeatList.add(id);
               } else if (line.contains("received heartbeat") && line.contains("FD_ALL::up::hdr")) {
                  Matcher m = Pattern.compile("\\((.*?)\\)").matcher(line);
                  List<String> matchList = new ArrayList<>();
                  while (m.find()) {
                     matchList.add(m.group(1));
                  }
                  // id
                  String id = matchList.get(matchList.size() - 1);
                  receivedHeartbeatList.add(id);
                  // elapsed
                  String elapsedMs = null;
                  m = Pattern.compile("\\d+ms").matcher(line);
                  if (m.find()) {
                     elapsedMs = m.group(0);
                  }
                  if (elapsedMs == null) {
                     throw new NullPointerException();
                  }
                  elapsedMs = elapsedMs.replaceAll("ms", "");
                  if (Integer.valueOf(elapsedMs) > elapsedFilter) {
                     elapsedList.add(elapsedMs);
                  }
               }
            });
            sentHeartbeat.put(fileName, sentHeartbeatList);
            receivedHeartbeat.put(fileName, receivedHeartbeatList);
            elapsed.put(fileName, elapsedList);
         }
      }

      System.out.println(elapsed);

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

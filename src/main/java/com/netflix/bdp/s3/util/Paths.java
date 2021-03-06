/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.bdp.s3.util;

import com.google.common.base.Objects;
import com.netflix.bdp.s3.S3Committer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import java.io.IOException;
import java.net.URI;
import java.util.Random;

public class Paths {
  public static String addUUID(String path, String uuid) {
    // In some cases, Spark will add the UUID to the filename itself.
    if (path.contains(uuid)) {
      return path;
    }

    int dot; // location of the first '.' in the file name
    int lastSlash = path.lastIndexOf('/');
    if (lastSlash >= 0) {
      dot = path.indexOf('.', lastSlash);
    } else {
      dot = path.indexOf('.');
    }

    if (dot >= 0) {
      return path.substring(0, dot) + "-" + uuid + path.substring(dot);
    } else {
      return path + "-" + uuid;
    }
  }

  private static class Pair<L, R> {
    private final L first;
    private final R second;

    public static <L, R> Pair<L, R> of(L first, R second) {
      return new Pair<>(first, second);
    }

    private Pair(L first, R second) {
      this.first = first;
      this.second = second;
    }

    public L getFirst() {
      return first;
    }

    public R getSecond() {
      return second;
    }
  }

  public static Path getRoot(Path path) {
    Path current = path;
    while (!current.isRoot()) {
      current = current.getParent();
    }
    return current;
  }

  public static Pair<String, String> splitFilename(String path) {
    int lastSlash = path.lastIndexOf('/');
    return Pair.of(path.substring(0, lastSlash), path.substring(lastSlash + 1));
  }

  public static String getParent(String path) {
    int lastSlash = path.lastIndexOf('/');
    if (lastSlash >= 0) {
      return path.substring(0, lastSlash);
    }
    return null;
  }

  public static String getFilename(String path) {
    int lastSlash = path.lastIndexOf('/');
    if (lastSlash >= 0) {
      return path.substring(lastSlash + 1);
    }
    return path;
  }

  public static String getRelativePath(Path basePath,
                                       Path fullPath) {
    // TODO: test this thoroughly
    // Use URI.create(Path#toString) to avoid URI character escape bugs
    URI relative = URI.create(basePath.toString())
        .relativize(URI.create(fullPath.toString()));
    return relative.getPath();
  }

  public static Path getLocalTaskAttemptTempDir(Configuration localConf,
                                                String uuid, int taskId,
                                                int attemptId) {
    return new Path(localTemp(localConf, taskId, attemptId), uuid);
  }

  public static Path getMultipartUploadCommitsDirectory(Configuration conf,
                                                        String uuid)
      throws IOException {
    // no need to use localTemp, this is HDFS in production        
    return new Path(getCommitsDirRoot(conf, uuid), "pending-uploads");
  }

  public static Path getCommitsDirRoot(Configuration conf, String uuid) throws IOException {
    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    String rootDir = S3Committer.DEFAULT_COMMITS_DIR + "-" + user;
    return FileSystem.get(conf).makeQualified(new Path(rootDir, uuid));
  }

  // TODO: verify this is correct, it comes from dse-storage
  private static Path localTemp(Configuration localConf, int taskId, int attemptId) {
    String localDirs = localConf.get("mapreduce.cluster.local.dir");
    Random rand = new Random(Objects.hashCode(taskId, attemptId));
    String[] dirs = localDirs.split(",");
    String dir = dirs[rand.nextInt(dirs.length)];

    try {
      return FileSystem.getLocal(localConf).makeQualified(new Path(dir));
    } catch (IOException e) {
      throw new RuntimeException("Failed to localize path: " + dir, e);
    }
  }

  public static String removeStartingAndTrailingSlash(String path) {
    int start = 0;
    if (path.startsWith("/")) {
      start = 1;
    }

    int end = path.length();
    if (path.endsWith("/")) {
      end -= 1;
    }

    return path.substring(start, end);
  }
}

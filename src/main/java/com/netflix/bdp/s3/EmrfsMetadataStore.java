package com.netflix.bdp.s3;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import com.google.common.collect.Lists;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * A class that sets EMRFS metadata for an S3 object.
 */
public class EmrfsMetadataStore implements Serializable {

  private AmazonDynamoDB dynamo;
  private String emrfsMetadataTable;
  private byte[] payloadForFilePut;
  private byte[] payloadForDirPut;
  private transient Configuration config;

  private static final String HASH_KEY_COL = "hashKey";
  private static final String RANGE_KEY_COL = "rangeKey";
  private static final String COUNTER_COL = "counter";
  private static final String DELETION_TTL_COL = "deletionTTL";
  private static final String LAST_MODIFIED_COL = "lastModified";
  private static final String PAYLOAD_COL = "payload";

  // maximum number of write operations in a batch for DynamoDB
  private static final int MAX_DYNAMO_BATCH_SIZE = 25;

  /**
   * Constructor.
   *
   * @param dynamo DynamoDB client
   */
  public EmrfsMetadataStore(AmazonDynamoDB dynamo) {
    this.dynamo = dynamo;
    initConfig();

    if (emrfsMetadataTable != null) {
      // only initialize the payload if we're using EMRFS
      this.payloadForFilePut = getPayloadForPut(false);
      this.payloadForDirPut = getPayloadForPut(true);
    }
  }

  private synchronized void initConfig() {
    if (config == null) {
      config = new Configuration();
      config.addResource("emrfs-site.xml");
      emrfsMetadataTable = config.get("fs.s3.consistent.metadata.tableName");
    }
  }

  /**
   * Stores EMRFS metadata for an S3 object. This will not create the metadata for parent directories.
   *
   * @param path The path of the S3 object
   */
  public void storeFileMetadata(Path path) {
    storeFileMetadata(path, false);
  }

  /**
   * Stores EMRFS metadata for an S3 object. This can optionally create the metadata for parent directories as well.
   *
   * @param path   The path of the S3 object
   * @param mkdirs True to create the metadata for all parent directories. false otherwise
   */
  public void storeFileMetadata(Path path, boolean mkdirs) {
    if (config == null) {
      initConfig();
    }

    if (emrfsMetadataTable == null) {
      return;
    }

    if (mkdirs) {
      List<WriteRequest> writes = createDirWriteRequests(path.getParent());
      writes.add(createWriteRequest(path, false));

      List<List<WriteRequest>> batches = Lists.partition(writes, MAX_DYNAMO_BATCH_SIZE);
      for (List<WriteRequest> batch : batches) {
        Map<String, List<WriteRequest>> map = new HashMap<>();
        map.put(emrfsMetadataTable, batch);
        BatchWriteItemResult result = dynamo.batchWriteItem(map);
        if (!result.getUnprocessedItems().isEmpty()) {
          throw new RuntimeException("Incomplete batch write to EMRFS dynamo table");
        }
      }
    } else {
      dynamo.putItem(emrfsMetadataTable, createPutItem(path, false));
    }
  }

  private List<WriteRequest> createDirWriteRequests(Path path) {
    List<WriteRequest> result = new ArrayList<>();

    Stack<Path> paths = new Stack<>();
    Path dir = path;
    do {
      paths.push(dir);
      dir = dir.getParent();
    } while (dir != null && dir.getParent() != null);

    while (!paths.empty()) {
      Path p = paths.pop();
      result.add(createWriteRequest(p, true));
    }

    return result;
  }

  private WriteRequest createWriteRequest(Path path, boolean isDirectory) {
    return new WriteRequest(new PutRequest(createPutItem(path, isDirectory)));
  }

  private Map<String, AttributeValue> createPutItem(Path path, boolean isDirectory) {
    String parentDir = path.getParent().toString();
    if (parentDir.startsWith("s3:/")) {
      parentDir = parentDir.substring(4);
    }
    String key = path.getName();

    Map<String, AttributeValue> values = new HashMap<>();
    values.put(HASH_KEY_COL, new AttributeValue(parentDir));
    values.put(RANGE_KEY_COL, new AttributeValue(key));

    AttributeValue counter = new AttributeValue();
    counter.setN(Long.toString(1L));
    values.put(COUNTER_COL, counter);

    AttributeValue delTtl = new AttributeValue();
    delTtl.setN(Long.toString(0L));
    values.put(DELETION_TTL_COL, delTtl);

    AttributeValue lastModified = new AttributeValue();
    lastModified.setN(Long.toString(System.currentTimeMillis()));
    values.put(LAST_MODIFIED_COL, lastModified);

    AttributeValue payload = new AttributeValue();
    if (isDirectory) {
      payload.setB(ByteBuffer.wrap(payloadForDirPut));
    } else {
      payload.setB(ByteBuffer.wrap(payloadForFilePut));
    }
    values.put(PAYLOAD_COL, payload);

    return values;
  }

  @SuppressWarnings("unchecked")
  private byte[] getPayloadForPut(boolean isDirectory) {
    try {
      Class clazz = Class.forName("com.amazon.ws.emr.hadoop.fs.EmrFsStore$MetadataFile");
      Object builder = clazz.getMethod("newBuilder").invoke(null);
      builder.getClass().getMethod("setIsDirectory", boolean.class).invoke(builder, isDirectory);
      Class stateEnumClass = Class.forName("com.amazon.ws.emr.hadoop.fs.EmrFsStore$MetadataFile$State");
      builder.getClass().getMethod("setState", stateEnumClass).invoke(builder, Enum.valueOf(stateEnumClass, "PUT"));
      builder.getClass().getMethod("setVersion", int.class).invoke(builder, 1);
      Object metadata = builder.getClass().getMethod("build").invoke(builder);
      return (byte[]) metadata.getClass().getMethod("toByteArray").invoke(metadata);
    } catch (Exception x) {
      throw new RuntimeException(x);
    }
  }
}


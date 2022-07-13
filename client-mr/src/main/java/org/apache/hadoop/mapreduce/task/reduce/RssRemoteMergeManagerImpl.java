/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapOutputFile;
import org.apache.hadoop.mapred.Merger;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.Progress;

import com.tencent.rss.common.exception.RssException;
import com.tencent.rss.storage.util.ShuffleStorageUtils;

public class RssRemoteMergeManagerImpl<K, V> extends MergeManagerImpl<K, V> {

  private static final Log LOG = LogFactory.getLog(RssRemoteMergeManagerImpl.class);

  /* Maximum percentage of the in-memory limit that a single shuffle can
   * consume*/
  private static final float DEFAULT_SHUFFLE_MEMORY_LIMIT_PERCENT
    = 0.25f;

  private final String appId;
  private final TaskAttemptID reduceId;

  private final JobConf jobConf;

  protected MapOutputFile mapOutputFile;

  Set<InMemoryMapOutput<K, V>> inMemoryMapOutputs =
    new TreeSet<InMemoryMapOutput<K,V>>(new MapOutput.MapOutputComparator<K, V>());
  private final RssInMemoryRemoteMerger<K, V> inMemoryMerger;

  Set<Path> onHDFSMapOutputs = new TreeSet<Path>();


  @VisibleForTesting
  final long memoryLimit;

  private long usedMemory;
  private long commitMemory;

  @VisibleForTesting
  final long maxSingleShuffleLimit;

  private final int memToMemMergeOutputsThreshold;
  private final long mergeThreshold;

  private final int ioSortFactor;

  private final Reporter reporter;
  private final ExceptionReporter exceptionReporter;

  /**
   * Combiner class to run during in-memory merge, if defined.
   */
  private final Class<? extends Reducer> combinerClass;

  /**
   * Resettable collector used for combine.
   */
  private final Task.CombineOutputCollector<K,V> combineCollector;

  private final Counters.Counter spilledRecordsCounter;

  private final Counters.Counter reduceCombineInputCounter;

  private final Counters.Counter mergedMapOutputsCounter;

  private final CompressionCodec codec;

  private final Progress mergePhase;

  private String basePath;
  private FileSystem remoteFS;

  public RssRemoteMergeManagerImpl(String appId, TaskAttemptID reduceId, JobConf jobConf,
                              String basePath,
                              FileSystem localFS,
                              LocalDirAllocator localDirAllocator,
                              Reporter reporter,
                              CompressionCodec codec,
                              Class<? extends Reducer> combinerClass,
                              Task.CombineOutputCollector<K,V> combineCollector,
                              Counters.Counter spilledRecordsCounter,
                              Counters.Counter reduceCombineInputCounter,
                              Counters.Counter mergedMapOutputsCounter,
                              ExceptionReporter exceptionReporter,
                              Progress mergePhase, MapOutputFile mapOutputFile) {
    super(reduceId,  jobConf,
      localFS,
      localDirAllocator,
      reporter,
      codec,
      combinerClass,
      combineCollector,
      spilledRecordsCounter,
      reduceCombineInputCounter,
      mergedMapOutputsCounter,
      exceptionReporter,
      mergePhase, mapOutputFile);
    // todo: clean code

    this.appId = appId;
    this.reduceId = reduceId;
    this.jobConf = jobConf;
    this.exceptionReporter = exceptionReporter;

    this.reporter = reporter;
    this.codec = codec;
    this.combinerClass = combinerClass;
    this.combineCollector = combineCollector;
    this.reduceCombineInputCounter = reduceCombineInputCounter;
    this.spilledRecordsCounter = spilledRecordsCounter;
    this.mergedMapOutputsCounter = mergedMapOutputsCounter;
    this.mapOutputFile = mapOutputFile;
    this.mapOutputFile.setConf(jobConf);

    try {
      this.remoteFS = ShuffleStorageUtils.getFileSystemForPath(new Path(basePath), jobConf);
    } catch (IOException e) {
      throw new RuntimeException("cannot ");
    }

    this.basePath = basePath;

    final float maxInMemCopyUse =
      jobConf.getFloat(MRJobConfig.SHUFFLE_INPUT_BUFFER_PERCENT,
        MRJobConfig.DEFAULT_SHUFFLE_INPUT_BUFFER_PERCENT);
    if (maxInMemCopyUse > 1.0 || maxInMemCopyUse < 0.0) {
      throw new IllegalArgumentException("Invalid value for "
        + MRJobConfig.SHUFFLE_INPUT_BUFFER_PERCENT + ": "
        + maxInMemCopyUse);
    }

    // Allow unit tests to fix Runtime memory
    this.memoryLimit = (long)(jobConf.getLong(
      MRJobConfig.REDUCE_MEMORY_TOTAL_BYTES,
      Runtime.getRuntime().maxMemory()) * maxInMemCopyUse);

    this.ioSortFactor = jobConf.getInt(MRJobConfig.IO_SORT_FACTOR, 100);

    final float singleShuffleMemoryLimitPercent =
      jobConf.getFloat(MRJobConfig.SHUFFLE_MEMORY_LIMIT_PERCENT,
        DEFAULT_SHUFFLE_MEMORY_LIMIT_PERCENT);
    if (singleShuffleMemoryLimitPercent <= 0.0f
      || singleShuffleMemoryLimitPercent > 1.0f) {
      throw new IllegalArgumentException("Invalid value for "
        + MRJobConfig.SHUFFLE_MEMORY_LIMIT_PERCENT + ": "
        + singleShuffleMemoryLimitPercent);
    }

    usedMemory = 0L;
    commitMemory = 0L;
    long maxSingleShuffleLimitConfiged =
      (long)(memoryLimit * singleShuffleMemoryLimitPercent);
    if (maxSingleShuffleLimitConfiged > Integer.MAX_VALUE) {
      maxSingleShuffleLimitConfiged = Integer.MAX_VALUE;
      LOG.info("The max number of bytes for a single in-memory shuffle cannot"
        + " be larger than Integer.MAX_VALUE. Setting it to Integer.MAX_VALUE");
    }
    this.maxSingleShuffleLimit = maxSingleShuffleLimitConfiged;
    this.memToMemMergeOutputsThreshold =
      jobConf.getInt(MRJobConfig.REDUCE_MEMTOMEM_THRESHOLD, ioSortFactor);
    this.mergeThreshold = (long)(this.memoryLimit
      * jobConf.getFloat(
        MRJobConfig.SHUFFLE_MERGE_PERCENT,
        MRJobConfig.DEFAULT_SHUFFLE_MERGE_PERCENT));
    LOG.info("MergerManager: memoryLimit=" + memoryLimit + ", "
      + "maxSingleShuffleLimit=" + maxSingleShuffleLimit + ", "
      + "mergeThreshold=" + mergeThreshold + ", "
      + "ioSortFactor=" + ioSortFactor + ", "
      + "memToMemMergeOutputsThreshold=" + memToMemMergeOutputsThreshold);

    if (this.maxSingleShuffleLimit >= this.mergeThreshold) {
      throw new RuntimeException("Invalid configuration: "
        + "maxSingleShuffleLimit should be less than mergeThreshold "
        + "maxSingleShuffleLimit: " + this.maxSingleShuffleLimit
        + "mergeThreshold: " + this.mergeThreshold);
    }

    this.inMemoryMerger = createRssInMemoryMerger();
    this.inMemoryMerger.start();
    this.mergePhase = mergePhase;
  }


  protected RssInMemoryRemoteMerger<K, V> createRssInMemoryMerger() {

    return new RssInMemoryRemoteMerger<K, V>(
      this,
      jobConf,
      remoteFS,
      new Path(basePath, appId),
      reduceId.toString(),
      codec,
      reporter,
      spilledRecordsCounter,
      combinerClass,
      exceptionReporter,
      combineCollector,
      reduceCombineInputCounter,
      mergedMapOutputsCounter
    );
  }

  @Override
  public synchronized MapOutput<K, V> reserve(TaskAttemptID mapId,
                                 long requestedSize,
                                 int fetcher) throws IOException {
    // we disable OnDisk MapOutput to avoid merging disk immediate data
    if (usedMemory > memoryLimit) {
      LOG.debug(mapId + ": Stalling shuffle since usedMemory (" + usedMemory
        + ") is greater than memoryLimit (" + memoryLimit + ")."
        + " CommitMemory is (" + commitMemory + ")");
      return null;
    }

    // Allow the in-memory shuffle to progress
    LOG.debug(mapId + ": Proceeding with shuffle since usedMemory ("
      + usedMemory + ") is lesser than memoryLimit (" + memoryLimit + ")."
      + "CommitMemory is (" + commitMemory + ")");
    usedMemory += requestedSize;
    // use this rss merger as the callback
    return new InMemoryMapOutput<K,V>(jobConf, mapId, this, (int)requestedSize, codec, true);
  }

  @Override
  public synchronized void closeInMemoryFile(InMemoryMapOutput<K,V> mapOutput) {

    inMemoryMapOutputs.add(mapOutput);
    LOG.info("closeInMemoryFile -> map-output of size: " + mapOutput.getSize()
      + ", inMemoryMapOutputs.size() -> " + inMemoryMapOutputs.size()
      + ", commitMemory -> " + commitMemory + ", usedMemory ->" + usedMemory);

    commitMemory+= mapOutput.getSize();
    // Can hang if mergeThreshold is really low.
    if (commitMemory >= mergeThreshold) {
      LOG.info("Starting inMemoryMerger's merge since commitMemory="
        + commitMemory + " > mergeThreshold=" + mergeThreshold
        + ". Current usedMemory=" + usedMemory);
      inMemoryMergedMapOutputs.clear();
      inMemoryMerger.startMerge(inMemoryMapOutputs);
      commitMemory = 0L;  // Reset commitMemory.
    }
    // we disable memToMemMerger to simplify design
  }

  public synchronized void closeOnHDFSFile(Path file) {
    onHDFSMapOutputs.add(file);
  }

    @Override
  public RawKeyValueIterator close() throws Throwable {
    // Wait for on-going merges to complete
    inMemoryMerger.startMerge(inMemoryMapOutputs);
    inMemoryMerger.close();
    if (!inMemoryMapOutputs.isEmpty()) {
      throw new RssException("InMemoryMapOutputs should be empty");
    }
    return finalMerge();
  }

  private RawKeyValueIterator finalMerge() throws IOException {
    Class<K> keyClass = (Class<K>)jobConf.getMapOutputKeyClass();
    Class<V> valueClass = (Class<V>)jobConf.getMapOutputValueClass();
    final RawComparator<K> comparator =
        (RawComparator<K>)jobConf.getOutputKeyComparator();
    // We will only merge sort once time
    return Merger.merge(jobConf, remoteFS, keyClass, valueClass, codec,
        onHDFSMapOutputs.toArray(new Path[onHDFSMapOutputs.size()]), true, Integer.MAX_VALUE,
        new Path("reduceId"), comparator, reporter, spilledRecordsCounter,
        null, null, null);
  }
}

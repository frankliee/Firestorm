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
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapOutputFile;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.Progress;

public class RssRemoteMergeManagerImpl<K, V> extends MergeManagerImpl<K, V> {

  private static final Log LOG = LogFactory.getLog(RssRemoteMergeManagerImpl.class);

  /* Maximum percentage of the in-memory limit that a single shuffle can
   * consume*/
  private static final float DEFAULT_SHUFFLE_MEMORY_LIMIT_PERCENT
    = 0.25f;

  private final TaskAttemptID reduceId;

  private final JobConf jobConf;
  private final FileSystem localFS;
  private final FileSystem rfs;
  private final LocalDirAllocator localDirAllocator;

  protected MapOutputFile mapOutputFile;

  Set<InMemoryMapOutput<K, V>> inMemoryMapOutputs =
    new TreeSet<InMemoryMapOutput<K,V>>(new MapOutput.MapOutputComparator<K, V>());
  private final MergeThread<InMemoryMapOutput<K,V>, K,V> inMemoryMerger;

  Set<CompressAwarePath> onDiskMapOutputs = new TreeSet<CompressAwarePath>();

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

  public RssRemoteMergeManagerImpl(TaskAttemptID reduceId, JobConf jobConf,
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

    this.reduceId = reduceId;
    this.jobConf = jobConf;
    this.localDirAllocator = localDirAllocator;
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

    this.localFS = localFS;
    this.rfs = ((LocalFileSystem)localFS).getRaw();

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

  protected MergeThread<InMemoryMapOutput<K,V>, K,V> createRssInMemoryMerger() {

    // todo: create RssInMemoryMerger
    return null;
  }

  @Override
  public synchronized MapOutput<K, V> reserve(TaskAttemptID mapId,
                                 long requestedSize,
                                 int fetcher) throws IOException {

    // todo: malloc full in-memory buffer to save fetched RSS data
    return null;
  }

  @Override
  public synchronized void closeInMemoryFile(InMemoryMapOutput<K,V> mapOutput) {

    // todo: check memory is full, then startMerge with RssInMemoryMerger
  }

    @Override
  public RawKeyValueIterator close() throws Throwable {
    // todo: return rssFinalMerge(in-memory, hdfs)
    return null;
  }
}

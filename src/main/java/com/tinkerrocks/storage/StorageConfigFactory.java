package com.tinkerrocks.storage;

import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

/**
 * Created by ashishn on 8/12/15.
 */
public class StorageConfigFactory {
    private static Options options;
    private static DBOptions dbOptions;
    //private static ColumnFamilyOptions columnFamilyOptions;
    private static WriteOptions writeOptions;
    private static ReadOptions readOptions;
    private static CompressionType compressionType = CompressionType.LZ4_COMPRESSION;


    public static Options getOptions() {
        if (options != null) {
            return options;
        }

        options = new Options()
                .setCreateIfMissing(true)

                .setWriteBufferSize(512 * SizeUnit.MB)
                .setMaxWriteBufferNumber(30)
                .setStatsDumpPeriodSec(100)
                .setMaxGrandparentOverlapFactor(10)
                .setMaxBackgroundCompactions(8)
                .setTargetFileSizeBase(64 * SizeUnit.MB)
                .setMaxBytesForLevelBase(512 * SizeUnit.MB)
                .setCompressionType(compressionType)
                .setCompactionStyle(CompactionStyle.LEVEL)
                .setIncreaseParallelism(10)
                .setMemtablePrefixBloomBits(8 * 1024 * 1024)
                .setMemtablePrefixBloomProbes(6)
                .setBloomLocality(1)
                .setTargetFileSizeMultiplier(1)
                .setMaxBytesForLevelMultiplier(10)
                .setAllowOsBuffer(true)
                .setLevelCompactionDynamicLevelBytes(true)
                .setTableCacheNumshardbits(10)
                .setLevelZeroFileNumCompactionTrigger(10)
                .setLevelZeroSlowdownWritesTrigger(20)
                .setLevelZeroStopWritesTrigger(12)
                .setMaxBackgroundFlushes(3)
                .setDisableDataSync(true)
                .setMaxOpenFiles(20000)
                .setAllowOsBuffer(true)
                .setSourceCompactionFactor(1)
                .setAllowMmapReads(false)
                .setAllowMmapWrites(false)
                .setFilterDeletes(false)
                .setDisableAutoCompactions(false)
                .setBytesPerSync(2 << 20)
                .setUseAdaptiveMutex(true)
                .setHardRateLimit(2)
                .setParanoidChecks(false)
                .optimizeLevelStyleCompaction();

        return options;
    }


    public static DBOptions getDBOptions() {
        if (dbOptions != null) {
            return dbOptions;
        }

        dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setMaxBackgroundCompactions(8)
                .setCreateMissingColumnFamilies(true)
                .setIncreaseParallelism(10)
                .setAllowOsBuffer(true)
                .setTableCacheNumshardbits(10)
                .setMaxBackgroundFlushes(3)
                .setDisableDataSync(true)
                .setMaxOpenFiles(20000)
                .setAllowOsBuffer(true)
                .setAllowMmapReads(false)
                .setAllowMmapWrites(false)
                .setBytesPerSync(2 << 20)
                .setUseAdaptiveMutex(true)
                .setParanoidChecks(false);

        return dbOptions;

    }


    public static ColumnFamilyOptions getColumnFamilyOptions() {
//        if (columnFamilyOptions != null) {
//            return columnFamilyOptions;
//        }

        ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions()
                .setWriteBufferSize(1024 * SizeUnit.MB)
                .setMaxWriteBufferNumber(30)
                .setMaxGrandparentOverlapFactor(10)
                .setTargetFileSizeBase(64 * SizeUnit.MB)
                .setMaxBytesForLevelBase(512 * SizeUnit.MB)
                .setCompactionStyle(CompactionStyle.LEVEL)
                .setMemtablePrefixBloomBits(8 * 1024 * 1024)
                .setMemtablePrefixBloomProbes(6)
                .setBloomLocality(1)
                .setLevelCompactionDynamicLevelBytes(true)
                .setTargetFileSizeMultiplier(1)
                .setMaxBytesForLevelMultiplier(10)
                .setLevelZeroFileNumCompactionTrigger(10)
                .setHardRateLimit(2)
                .setMinWriteBufferNumberToMerge(2)
                .setMaxWriteBufferNumber(6)
                .setInplaceUpdateSupport(true)
                .setOptimizeFiltersForHits(true)
                .setCompressionType(compressionType);
        //.optimizeLevelStyleCompaction();


        columnFamilyOptions.setMemTableConfig(new SkipListMemTableConfig());

        BlockBasedTableConfig table_options = new BlockBasedTableConfig();

        Filter bloomFilter = new BloomFilter(10, true);

        table_options.setBlockCacheSize(512 * SizeUnit.MB)
                .setBlockCacheCompressedNumShardBits(8)
                .setHashIndexAllowCollision(false)
                .setFilter(bloomFilter)
                .setBlockSize(4096)
                .setBlockSizeDeviation(5)
                .setBlockRestartInterval(10)
                .setBlockCacheCompressedSize(128 * SizeUnit.KB)
                .setCacheNumShardBits(8);

        columnFamilyOptions.setTableFormatConfig(table_options);


        return columnFamilyOptions;
    }

    public static WriteOptions getWriteOptions() {
        if (writeOptions != null) {
            return writeOptions;
        }
        writeOptions = new WriteOptions();
        writeOptions.setSync(false);
        return writeOptions;
    }


    public static ReadOptions getReadOptions() {
        if (readOptions != null) {
            return readOptions;
        }
        readOptions = new ReadOptions();
        readOptions.setVerifyChecksums(false);

        return readOptions;
    }


}

package com.tinkerrocks.storage;

import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

/**
 * Created by ashishn on 8/12/15.
 */
public class StorageConfigFactory {
    static Options options;
    static DBOptions dbOptions;
    static ColumnFamilyOptions columnFamilyOptions;
    static WriteOptions writeOptions;


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
                .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
                .setCompactionStyle(CompactionStyle.LEVEL)
                .setIncreaseParallelism(10)
                .setMemtablePrefixBloomBits(8 * 1024 * 1024)
                .setMemtablePrefixBloomProbes(6)
                .setBloomLocality(1)
                .setTargetFileSizeMultiplier(1)
                .setMaxBytesForLevelMultiplier(10)
                .setAllowOsBuffer(true)
                .setTableCacheNumshardbits(10)
                .setLevelZeroFileNumCompactionTrigger(10)
                .setLevelZeroSlowdownWritesTrigger(20)
                .setLevelZeroStopWritesTrigger(12)
                .setMaxBackgroundFlushes(3)
                .setDisableDataSync(true)
                .setMaxOpenFiles(20000)
                .setNumLevels(8)
                .setAllowOsBuffer(true)
                .setSourceCompactionFactor(1)
                .setAllowMmapReads(false)
                .setAllowMmapWrites(false)
                .setFilterDeletes(false)
                .setDisableAutoCompactions(false)
                .setBytesPerSync(2 << 20)
                .optimizeForPointLookup(4096)
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
        if (columnFamilyOptions != null) {
            return columnFamilyOptions;
        }

        columnFamilyOptions = new ColumnFamilyOptions()
                .setWriteBufferSize(512 * SizeUnit.MB)
                .setMaxWriteBufferNumber(30)
                .setMaxGrandparentOverlapFactor(10)
                .setTargetFileSizeBase(64 * SizeUnit.MB)
                .setMaxBytesForLevelBase(512 * SizeUnit.MB)
                .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
                .setCompactionStyle(CompactionStyle.LEVEL)
                .setMemtablePrefixBloomBits(8 * 1024 * 1024)
                .setMemtablePrefixBloomProbes(6)
                .setBloomLocality(1)
                .setTargetFileSizeMultiplier(1)
                .setMaxBytesForLevelMultiplier(10)
                .setLevelZeroFileNumCompactionTrigger(10)
                .setLevelZeroSlowdownWritesTrigger(20)
                .setLevelZeroStopWritesTrigger(12)
                .setNumLevels(8)
                .setSourceCompactionFactor(1)
                .setFilterDeletes(false)
                .setDisableAutoCompactions(false)
                .optimizeForPointLookup(4096)
                .setHardRateLimit(2)
                .optimizeLevelStyleCompaction();
        return columnFamilyOptions;
    }

    public static WriteOptions getWriteOptions() {
        if (writeOptions != null) {
            return writeOptions;
        }
        writeOptions = new WriteOptions();
        writeOptions.setSync(false);
        writeOptions.setDisableWAL(true);
        return writeOptions;
    }


}

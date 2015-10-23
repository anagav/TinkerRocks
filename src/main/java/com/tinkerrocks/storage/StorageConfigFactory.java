package com.tinkerrocks.storage;

import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

/**
 * Created by ashishn on 8/12/15.
 */
public class StorageConfigFactory {
    private static DBOptions dbOptions;
    private static ColumnFamilyOptions columnFamilyOptions;
    private static WriteOptions writeOptions;
    private static ReadOptions readOptions;
    private static CompressionType compressionType = CompressionType.SNAPPY_COMPRESSION;

    public static DBOptions getDBOptions() {
        if (dbOptions != null) {
            return dbOptions;
        }

        dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setIncreaseParallelism(16)
                .setParanoidChecks(false)
                .setAdviseRandomOnOpen(true)
                .setMaxBackgroundCompactions(10)
                .setCreateMissingColumnFamilies(true)
                .setAllowOsBuffer(true)
                .setTableCacheNumshardbits(10)
                .setMaxBackgroundFlushes(10)
                .setWalTtlSeconds(5 * 60)
                        //todo check later for performance
                .setMaxTotalWalSize(30 * SizeUnit.GB)
                .setMaxOpenFiles(-1)
                .setDisableDataSync(true)
                .setDeleteObsoleteFilesPeriodMicros(5 * 60 * 1000 * 1000)
                .setAllowOsBuffer(true)
                .setUseFsync(false)
                .setBytesPerSync(2 << 20)
                .setUseAdaptiveMutex(true);

        return dbOptions;

    }


    public static ColumnFamilyOptions getColumnFamilyOptions() {
        if (columnFamilyOptions != null) {
            return columnFamilyOptions;
        }


        BlockBasedTableConfig table_options = new BlockBasedTableConfig();

        Filter bloomFilter = new BloomFilter(10, true);

        table_options.setBlockCacheSize(512 * SizeUnit.MB)
                .setBlockCacheCompressedNumShardBits(8)
                .setFilter(bloomFilter)
                .setBlockSize(4096)
                .setBlockSizeDeviation(5)
                .setBlockRestartInterval(10)
                .setBlockCacheCompressedSize(128 * SizeUnit.KB)
                .setCacheNumShardBits(8);

        columnFamilyOptions = new ColumnFamilyOptions()
                .setWriteBufferSize(512 * SizeUnit.MB)
                .setMaxWriteBufferNumber(20)
                .setMinWriteBufferNumberToMerge(2)
                .setMaxWriteBufferNumber(6)
                .setMaxGrandparentOverlapFactor(10)
                .setTargetFileSizeBase(128 * SizeUnit.MB)
                .setMaxBytesForLevelBase(512 * SizeUnit.MB)
                .setCompactionStyle(CompactionStyle.LEVEL)
                .setMemtablePrefixBloomBits(8 * 1024 * 1024)
                .setMemtablePrefixBloomProbes(6)
                .setBloomLocality(1)
                .setInplaceUpdateSupport(true)
                .setPurgeRedundantKvsWhileFlush(true)
                .setDisableAutoCompactions(false)
                .setFilterDeletes(true)
                .setInplaceUpdateSupport(true)
                .setLevelCompactionDynamicLevelBytes(true)
                .setMaxBytesForLevelMultiplier(10)
                .setLevelZeroFileNumCompactionTrigger(10)
                .setHardRateLimit(2)
                .setCompressionType(compressionType)
                .setMemTableConfig(new SkipListMemTableConfig())
                .setTableFormatConfig(table_options);

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

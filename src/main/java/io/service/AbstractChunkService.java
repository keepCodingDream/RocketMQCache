package io.service;

import io.helper.Config;
import io.helper.Constants;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractChunkService implements IChunkService {

    protected static final Map<Integer, FileChannel> FILE_HOLDER = new ConcurrentHashMap<>(Config.TOTAL_CHUNK);
    /**
     * 存放TOTAL_CHUNK个文件句柄
     */
    protected static final Map<Integer, RandomAccessFile> FILE_ORI_HOLDER = new ConcurrentHashMap<>(Config.TOTAL_CHUNK);

    protected static final byte[] EMPTY_ITEM = new byte[0];

    /**
     * 保存每个chunk 内已经被分配的地址
     */
    protected static final Map<Integer, AtomicLong> CHUNK_INDEX = new ConcurrentHashMap<>(Config.TOTAL_CHUNK);

    public AbstractChunkService() {
        if (System.getProperty("os.name").contains("Mac")) {
            Config.BASE_PATH = "/Users/lurenjie/test_data/";
        }
        //初始化文件句柄
        for (int i = 0; i < Config.TOTAL_CHUNK; i++) {
            try {
                File file = new File(getFilePath(String.valueOf(i)) + Constants.DATA_FILE_SUFFIX);
                if (!file.exists()) {
                    if (!file.getParentFile().exists()) {
                        if (!file.getParentFile().mkdirs()) {
                            throw new RuntimeException("创建文件夹失败");
                        }
                    }
                    if (!file.createNewFile()) {
                        throw new RuntimeException("创建文件失败");
                    }
                }
                RandomAccessFile acf = new RandomAccessFile(file, "rw");
                FileChannel fc = acf.getChannel();
                FILE_ORI_HOLDER.put(i, acf);
                FILE_HOLDER.put(i, fc);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        //初始化index reference
        //维护一个当前索引文件写到哪里的内存索引
        for (int i = 0; i < Config.TOTAL_CHUNK; i++) {
            CHUNK_INDEX.put(i, new AtomicLong(0));
        }
    }

    private static String getFilePath(String queueName) {
        return Config.BASE_PATH + "/" + queueName;
    }

    /**
     * 获取制定消息的落盘索引地址
     *
     * @param offset 消息偏移量
     * @return 返回的数组第一个下标是开始位，第二个是结束位
     */
    protected long[] findMsgRange(long offset, ByteBuffer indexes) {
        long[] result = new long[3];
        if (indexes == null || indexes.position() == 0) {
            throw new RuntimeException("index error!");
        }
        //处于第几个写入分片其实是知道的
        int myIndex = (int) offset / Config.EACH_QUEUE_SIZE;
        int arrayIndex = myIndex * 2;
        int indexStart = arrayIndex * 8;
        int indexEnd = indexStart + 8;
        if (indexEnd > indexes.position()) {
            return null;
        }
        result[0] = indexes.getLong(indexStart);
        result[1] = indexes.getLong(indexStart + 8);
        result[2] = Config.EACH_QUEUE_SIZE * myIndex;
        return result;
    }
}

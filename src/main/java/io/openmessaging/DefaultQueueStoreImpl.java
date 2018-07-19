package io.openmessaging;

import io.helper.Config;
import io.helper.MemoryPool;
import io.service.BufferChunkService;
import io.service.IChunkService;

import java.util.Collection;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 *
 * @author lurenjie
 */
public class DefaultQueueStoreImpl extends QueueStore {

    private static IChunkService CHUNK_SERVER = new BufferChunkService();

    static {
        if (System.getProperty("os.name").contains("Mac")) {
            Config.TOTAL_TOPIC = 500000;
        }
        MemoryPool.init();
    }


    @Override
    public void put(String queueName, byte[] message) {
        try {
            CHUNK_SERVER.processData(queueName, message);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Collection<byte[]> get(String queueName, long offset, long num) {
        try {
            return CHUNK_SERVER.queryData(queueName, offset, num);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

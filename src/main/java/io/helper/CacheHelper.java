package io.helper;

import io.service.BufferChunkService;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 因为程序读、写都是一页一页，所以这完全可以做一个缓存
 */
public class CacheHelper {

    private static Map<String, ByteBuffer> QUEUE_MEMORY;
    private static Map<String, Long> CURRENT_INDEX;
    private static Map<String, ByteBuffer> CONTENT;

    public CacheHelper() {
        QUEUE_MEMORY = BufferChunkService.QUEUE_MEMORY;
        CONTENT = new ConcurrentHashMap<>(Config.TOTAL_TOPIC);
        CURRENT_INDEX = new ConcurrentHashMap<>(Config.TOTAL_TOPIC);
    }

    public void saveCache(String queueName, long start, byte[] data) {
        ByteBuffer object = CONTENT.get(queueName);
        if (object == null) {
            ByteBuffer buffer = QUEUE_MEMORY.get(queueName);
            buffer.clear();
            buffer.put(data);
            CONTENT.put(queueName, buffer);
            CURRENT_INDEX.put(queueName, start);
        } else {
            object.clear();
            object.put(data);
            CURRENT_INDEX.put(queueName, start);
        }
    }

    public byte[] findCache(String queueName, long start) {
        if (CURRENT_INDEX.containsKey(queueName) && start == CURRENT_INDEX.get(queueName)) {
            ByteBuffer byteBuffer = CONTENT.get(queueName);
            if (byteBuffer.isDirect()) {
                byte[] content = new byte[byteBuffer.position()];
                byteBuffer.flip();
                byteBuffer.get(content);
                byteBuffer.limit(byteBuffer.capacity());
                return content;
            } else {
                return byteBuffer.array();
            }
        }
        return null;
    }

}

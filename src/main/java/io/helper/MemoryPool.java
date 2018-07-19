package io.helper;

import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingDeque;

public class MemoryPool {
    private static final LinkedBlockingDeque<ByteBuffer> QUEUE_MEMORY_HOLDER = new LinkedBlockingDeque<>(Config.TOTAL_TOPIC);
    private static final LinkedBlockingDeque<ByteBuffer> INDEX_MEMORY_HOLDER = new LinkedBlockingDeque<>(Config.TOTAL_TOPIC);

    public static void init() {
        for (int i = 0; i < Config.TOTAL_TOPIC; i++) {
            if (i % 3 == 0) {
                QUEUE_MEMORY_HOLDER.add(ByteBuffer.allocate(Config.EACH_PAGE_SIZE));
            } else {
                QUEUE_MEMORY_HOLDER.add(ByteBuffer.allocateDirect(Config.EACH_PAGE_SIZE));
            }
            INDEX_MEMORY_HOLDER.add(ByteBuffer.allocateDirect(666));
        }
    }

    public static ByteBuffer getByteBuffer() {
        try {
            return QUEUE_MEMORY_HOLDER.takeFirst();
        } catch (InterruptedException e) {
            return null;
        }
    }

    public static ByteBuffer getIndexByteBuffer() {
        try {
            return INDEX_MEMORY_HOLDER.takeFirst();
        } catch (InterruptedException e) {
            return null;
        }
    }
}

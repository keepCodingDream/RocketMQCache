package io.service;

import io.helper.Config;
import io.helper.MemoryPool;
import io.impl.RandomMapFileOperation;
import io.impl.SequenceReadFIleOperation;
import io.operation.FileOperation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 写入数据基于 ByteBuffer.allocateDirect,使用堆外内存暂存队列数据，在到达 AbstractChunkService.EACH_QUEUE_SIZE 后写入。
 * 写入数据使用RandomAccessFile顺序写入
 * <p>
 * 数据读取采用磁盘顺序读，TPS 大概120万左右，也不太合格
 */
public class BufferChunkService extends AbstractChunkService {

    /**
     * topic 缓存
     */
    public static final Map<String, ByteBuffer> QUEUE_MEMORY = new ConcurrentHashMap<>(Config.TOTAL_TOPIC);

    /**
     * topic 消息累加器
     */
    private static final Map<String, AtomicInteger> QUEUE_COUNTER = new ConcurrentHashMap<>(Config.TOTAL_TOPIC);

    /**
     * topic 地址缓存
     */
    private static final Map<String, ByteBuffer> QUEUE_INDEXES = new ConcurrentHashMap<>(Config.TOTAL_TOPIC);

    /**
     * 负责从ByteBuffer copy到 byte的线程池
     */
    private static final ExecutorService READ_WRITE_THREAD = Executors.newFixedThreadPool(10);

    private static FileOperation<byte[]> SEQUENCE_FILE_OPERATION;
    private static FileOperation<ByteBuffer> BUFFER_FILE_OPERATION;

    private static final AtomicBoolean READ_FLAG = new AtomicBoolean(true);
    private static ThreadLocal<SaveThread> SAVE_HOLDER = new ThreadLocal<>();
    private static ThreadLocal<ReadThread> READ_HOLDER = new ThreadLocal<>();

    public BufferChunkService() {
        SEQUENCE_FILE_OPERATION = new SequenceReadFIleOperation(FILE_HOLDER, FILE_ORI_HOLDER);
        BUFFER_FILE_OPERATION = new RandomMapFileOperation(FILE_HOLDER, FILE_ORI_HOLDER);
    }

    @Override
    public void processData(String queueName, byte[] data) throws Exception {
        int index = Math.abs(queueName.hashCode()) % Config.TOTAL_CHUNK;
        if (!QUEUE_MEMORY.containsKey(queueName)) {
            QUEUE_MEMORY.putIfAbsent(queueName, MemoryPool.getByteBuffer());
        }
        if (!QUEUE_COUNTER.containsKey(queueName)) {
            QUEUE_COUNTER.putIfAbsent(queueName, new AtomicInteger(0));
        }
        synchronized (QUEUE_MEMORY.get(queueName)) {
            ByteBuffer queue = QUEUE_MEMORY.get(queueName);
            queue.putInt(data.length);
            queue.put(data);
            long counter = QUEUE_COUNTER.get(queueName).incrementAndGet();
            if (counter % Config.EACH_QUEUE_SIZE == 0) {
                persistence(index, queueName, queue);
            }
        }
    }

    @Override
    public List<byte[]> queryData(String queueName, long offset, long num) throws Exception {
        if (READ_FLAG.get()) {
            synchronized (READ_FLAG) {
                if (READ_FLAG.get()) {
                    //cleanBuffer();
                    READ_FLAG.compareAndSet(true, false);
                }
            }
        }
        if (!QUEUE_MEMORY.containsKey(queueName) || offset >= 2000) {
            return Collections.emptyList();
        }
        ByteBuffer indexes = QUEUE_INDEXES.get(queueName);
        List<byte[]> result = new ArrayList<>((int) num);
        long latestIndex = 0;
        if (indexes != null && indexes.position() > 0) {
            int index = Math.abs(queueName.hashCode()) % Config.TOTAL_CHUNK;
            latestIndex = (indexes.position() / 16) * Config.EACH_QUEUE_SIZE;
            while (result.size() < num && offset < latestIndex) {
                long[] range = findMsgRange(offset, indexes);
                if (range == null) {
                    break;
                }
                int start = (int) (offset - range[2]);
                int end = (int) (start + num) - result.size();
                if (end > Config.EACH_QUEUE_SIZE) {
                    end = Config.EACH_QUEUE_SIZE;
                }
                if (end > start && start >= 0) {
                    List<byte[]> data = doRead(index, range[0], range[1], start, end, queueName);
                    if (data == null || data.size() == 0) {
                        break;
                    }
                    result.addAll(data);
                    offset += data.size();
                } else {
                    break;
                }
            }
        }
        if (result.size() < num) {
            //check data in QUEUE_MEMORY
            synchronized (QUEUE_MEMORY.get(queueName)) {
                ByteBuffer queue = QUEUE_MEMORY.get(queueName);
                int position = queue.position();
                queue.flip();
                int memStart = (int) (offset - latestIndex);
                int memEnd = memStart + (int) (num - result.size());
                if (memStart >= 0 && memEnd > memStart) {
                    result.addAll(convertB2BBDirM(queue, memStart, memEnd));
                }
                queue.position(position);
                queue.limit(queue.capacity());
            }
        }
        return result;
    }

    private void cleanBuffer() throws Exception {
        long startTime = System.nanoTime();
        for (Map.Entry<String, ByteBuffer> entry : QUEUE_MEMORY.entrySet()) {
            String queueName = entry.getKey();
            ByteBuffer queue = entry.getValue();
            if (queue.position() > 0) {
                int index = Math.abs(queueName.hashCode()) % Config.TOTAL_CHUNK;
                synchronized (QUEUE_INDEXES.get(queueName)) {
                    persistence(index, queueName, queue);
                }
            }
        }
        System.out.println("clean end!" + (System.nanoTime() - startTime));
    }

    /**
     * 执行持久化操作
     */
    private void persistence(int index, String queueName, ByteBuffer queue) throws Exception {
        long startIndex;
        long endIndex;
        synchronized (CHUNK_INDEX.get(index)) {
            AtomicLong chunkIndex = CHUNK_INDEX.get(index);
            startIndex = chunkIndex.longValue();
            //每次一整页写入，方便几个页一起读
            endIndex = startIndex + Config.EACH_PAGE_SIZE;
            chunkIndex.compareAndSet(startIndex, endIndex);
        }
        if (!QUEUE_INDEXES.containsKey(queueName)) {
            QUEUE_INDEXES.put(queueName, MemoryPool.getIndexByteBuffer());
        }
        ByteBuffer indexes = QUEUE_INDEXES.get(queueName);
        indexes.putLong(startIndex);
        indexes.putLong(endIndex);
        SaveThread saveThread = SAVE_HOLDER.get();
        if (saveThread == null) {
            saveThread = new SaveThread(queue, index, startIndex, endIndex);
            SAVE_HOLDER.set(saveThread);
        } else {
            saveThread.setEnd(endIndex);
            saveThread.setQueue(queue);
            saveThread.setIndex(index);
            saveThread.setStart(startIndex);
        }
        READ_WRITE_THREAD.submit(saveThread).get();
        queue.clear();
    }

    private class SaveThread implements Runnable {
        ByteBuffer queue;
        int index;
        long start;
        long end;

        public void setQueue(ByteBuffer queue) {
            this.queue = queue;
        }

        public void setIndex(int index) {
            this.index = index;
        }

        public void setStart(long start) {
            this.start = start;
        }

        public void setEnd(long end) {
            this.end = end;
        }

        SaveThread(ByteBuffer queue, int index, long start, long end) {
            this.queue = queue;
            this.index = index;
            this.start = start;
            this.end = end;
        }

        @Override
        public void run() {
            try {
                if (queue.isDirect()) {
                    BUFFER_FILE_OPERATION.doSave(index, start, end, queue);
                } else {
                    SEQUENCE_FILE_OPERATION.doSave(index, start, end, queue.array());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    private class ReadThread implements Callable<List<byte[]>> {
        int index;
        int start;
        int end;
        long startOffset;
        long endOffset;
        String queueName;

        public void setIndex(int index) {
            this.index = index;
        }

        public void setStart(int start) {
            this.start = start;
        }

        public void setEnd(int end) {
            this.end = end;
        }

        public void setStartOffset(long startOffset) {
            this.startOffset = startOffset;
        }

        public void setEndOffset(long endOffset) {
            this.endOffset = endOffset;
        }

        public void setQueueName(String queueName) {
            this.queueName = queueName;
        }

        ReadThread(int index, long startOffset, long endOffset, int start, int end, String queueName) {
            this.index = index;
            this.start = start;
            this.end = end;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.queueName = queueName;
        }

        @Override
        public List<byte[]> call() throws Exception {
            return doRead(index, startOffset, endOffset, start, end, queueName);
        }
    }

    /**
     * 读文件
     *
     * @param index       文件所属分区
     * @param startOffset 硬盘地址偏移 开始
     * @param endOffset   硬盘地址偏移 结束
     * @param start       第几个消息 开始 包含
     * @param end         第几个消息 结束 不包含
     */
    private List<byte[]> doRead(int index, long startOffset, long endOffset, int start, int end, String queueName) throws
            IOException {
        return SEQUENCE_FILE_OPERATION.doRead(queueName, index, startOffset, endOffset, start, end);
    }


    private List<byte[]> convertB2BBDirM(ByteBuffer byteBuffer, int start, int end) {
        List<byte[]> result = new ArrayList<>(end - start);
        int index = 0;
        while (byteBuffer.remaining() >= 4 && index < end) {
            int length = byteBuffer.getInt();
            if (length == 0) {
                if (index >= start) {
                    result.add(EMPTY_ITEM);
                }
                index++;
                continue;
            }
            if (index >= start) {
                if (byteBuffer.isDirect()) {
                    byte[] content = new byte[length];
                    int itemIndex = byteBuffer.position();
                    for (int i = 0; i < length; i++) {
                        content[i] = byteBuffer.get(itemIndex++);
                    }
                    byteBuffer = (ByteBuffer) byteBuffer.position(itemIndex);
                    result.add(content);
                } else {
                    byte[] content = new byte[length];
                    byteBuffer = byteBuffer.get(content, 0, length);
                    result.add(content);
                }
            } else {
                int currentP = byteBuffer.position();
                byteBuffer = (ByteBuffer) byteBuffer.position(currentP + length);
            }
            index++;
        }
        return result;
    }
}

package io.service;

import io.helper.BytesHelper;
import io.helper.Config;
import io.impl.SequenceReadFIleOperation;
import io.operation.FileOperation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 用内存接住byte list,然后直接刷到硬盘，看上去比用堆外内存靠谱
 */
public class MemSaveChunkService extends AbstractChunkService {
    /**
     * topic 缓存
     */
    private static final Map<String, List<byte[]>> QUEUE_MEMORY = new ConcurrentHashMap<>(Config.TOTAL_TOPIC);

    /**
     * topic 地址缓存
     */
    private static final Map<String, List<Long>> QUEUE_INDEXES = new ConcurrentHashMap<>(Config.TOTAL_TOPIC);

    private static final ThreadPoolExecutor WRITE_THREAD = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);
    private static final ThreadPoolExecutor READ_THREAD = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);

    private static FileOperation<byte[]> SEQUENCE_FILE_OPERATION;

    public MemSaveChunkService() {
        SEQUENCE_FILE_OPERATION = new SequenceReadFIleOperation(FILE_HOLDER, FILE_ORI_HOLDER);
    }

    private ThreadLocal<SaveThread> SAVE_THREAD = new ThreadLocal<>();

    private static final AtomicBoolean WRITE_STATUS = new AtomicBoolean(true);

    @Override
    public void processData(String queueName, byte[] data) throws Exception {
        int index = Math.abs(queueName.hashCode()) % Config.TOTAL_CHUNK;
        if (!QUEUE_MEMORY.containsKey(queueName)) {
            QUEUE_MEMORY.put(queueName, new ArrayList<>(Config.EACH_QUEUE_SIZE));
        }
        synchronized (QUEUE_MEMORY.get(queueName)) {
            List<byte[]> queue = QUEUE_MEMORY.get(queueName);
            queue.add(data);
            if (queue.size() % Config.EACH_QUEUE_SIZE == 0) {
                long startIndex;
                long endIndex;
                synchronized (CHUNK_INDEX.get(index)) {
                    AtomicLong chunkIndex = CHUNK_INDEX.get(index);
                    startIndex = chunkIndex.longValue();
                    endIndex = startIndex + findListLength(queue);
                    chunkIndex.compareAndSet(startIndex, endIndex);
                }
                if (!QUEUE_INDEXES.containsKey(queueName)) {
                    QUEUE_INDEXES.put(queueName, new ArrayList<>(Config.EACH_QUEUE_SIZE));
                }
                List<Long> indexes = QUEUE_INDEXES.get(queueName);
                indexes.add(startIndex);
                indexes.add(endIndex);
                byte[] content = new byte[Config.EACH_PAGE_SIZE];
                int current = 0;
                for (byte[] item : queue) {
                    byte[] itemLength = BytesHelper.intToByteArray(item.length);
                    for (byte anItemLength : itemLength) {
                        content[current++] = anItemLength;
                    }
                    for (byte anItem : item) {
                        content[current++] = anItem;
                    }
                }
                SaveThread saveThread = SAVE_THREAD.get();
                if (saveThread == null) {
                    saveThread = new SaveThread(content, index, startIndex, endIndex);
                    SAVE_THREAD.set(saveThread);
                } else {
                    saveThread.setEnd(endIndex);
                    saveThread.setStart(startIndex);
                    saveThread.setIndex(index);
                    saveThread.setQueue(content);
                }
                WRITE_THREAD.submit(saveThread);
                queue.clear();
            }
        }
    }

    @Override
    public List<byte[]> queryData(String queueName, long offset, long num) throws Exception {
        if (WRITE_STATUS.get()) {
            synchronized (WRITE_THREAD) {
                while (WRITE_THREAD.getActiveCount() > 0) {
                    WRITE_THREAD.shutdownNow();
                    Thread.sleep(10);
                }
                WRITE_STATUS.compareAndSet(true, false);
            }
        }
        if (!QUEUE_MEMORY.containsKey(queueName)) {
            return Collections.emptyList();
        }
        List<Long> indexes = QUEUE_INDEXES.get(queueName);
        List<byte[]> result = new ArrayList<>((int) num);
        long latestIndex = 0;
        int index = Math.abs(queueName.hashCode()) % Config.TOTAL_CHUNK;
        if (indexes != null && indexes.size() > 0) {
            latestIndex = (indexes.size() - 1) * Config.EACH_QUEUE_SIZE;
            while (result.size() < num && offset < latestIndex) {
//                long[] range = findMsgRange(offset, indexes);
                long[] range = null;
                if (range == null) {
                    break;
                }
                int start = (int) (offset - range[2]);
                int end = (int) (start + num) - result.size();
                if (end > Config.EACH_QUEUE_SIZE) {
                    end = Config.EACH_QUEUE_SIZE;
                }
                if (end > start && start >= 0) {
                    List<byte[]> data = READ_THREAD.submit(new ReadThread(index, range[0], range[1], start, end)).get();
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
                List<byte[]> queue = QUEUE_MEMORY.get(queueName);
                int memStart = (int) (offset - latestIndex);
                int memEnd = memStart + (int) (num - result.size());
                if (memEnd > queue.size()) {
                    memEnd = queue.size();
                }
                if (memEnd > memStart && memStart >= 0) {
                    result.addAll(queue.subList(memStart, memEnd));
                }
            }
        }
        return result;
    }

    /**
     * 获取数组总长度
     */
    private int findListLength(List<byte[]> data) {
        int totalLength = 0;
        for (byte[] aData : data) {
            totalLength += 4;
            totalLength += aData.length;
        }
        return totalLength;
    }

    private class SaveThread implements Callable<Long> {
        byte[] queue;
        int index;
        long start;
        long end;

        public void setQueue(byte[] queue) {
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

        SaveThread(byte[] queue, int index, long start, long end) {
            this.queue = queue;
            this.index = index;
            this.start = start;
            this.end = end;
        }

        @Override
        public Long call() throws Exception {
            return SEQUENCE_FILE_OPERATION.doSave(index, start, end, queue);
        }
    }

    private class ReadThread implements Callable<List<byte[]>> {
        int index;
        int start;
        int end;
        long startOffset;
        long endOffset;

        ReadThread(int index, long startOffset, long endOffset, int start, int end) {
            this.index = index;
            this.start = start;
            this.end = end;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
        }

        @Override
        public List<byte[]> call() throws Exception {
            return SEQUENCE_FILE_OPERATION.doRead("", index, startOffset, endOffset, start, end);
        }
    }

}

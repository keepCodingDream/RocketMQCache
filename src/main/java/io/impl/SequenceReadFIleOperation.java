package io.impl;

import io.helper.BytesHelper;
import io.helper.CacheHelper;
import io.helper.Config;
import io.operation.AbstractFileOperation;
import io.service.BufferChunkService;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SequenceReadFIleOperation extends AbstractFileOperation<byte[]> {

    private static final CacheHelper CACHE_HELPER = new CacheHelper();

    public SequenceReadFIleOperation(Map<Integer, FileChannel> FILE_HOLDER, Map<Integer, RandomAccessFile> FILE_ORI_HOLDER) {
        super(FILE_HOLDER, FILE_ORI_HOLDER);
    }

    @Override
    public long doSave(int index, long startOffset, long endOffset, byte[] data) throws IOException {
        synchronized (FILE_ORI_HOLDER.get(index)) {
            RandomAccessFile file = FILE_ORI_HOLDER.get(index);
            file.seek(startOffset);
            file.write(data);
            return System.nanoTime();
        }
    }

    @Override
    public List<byte[]> doRead(String queueName, int index, long startOffset, long endOffset, int start, int end) throws IOException {
        synchronized (BufferChunkService.QUEUE_MEMORY.get(queueName)) {
            byte[] content = CACHE_HELPER.findCache(queueName, startOffset);
            if (content == null) {
                synchronized (FILE_ORI_HOLDER.get(index)) {
                    content = CACHE_HELPER.findCache(queueName, startOffset);
                    if (content == null) {
                        RandomAccessFile file = FILE_ORI_HOLDER.get(index);
                        file.seek(startOffset);
                        content = new byte[Config.EACH_PAGE_SIZE];
                        file.read(content);
                    }
                }
                CACHE_HELPER.saveCache(queueName, startOffset, content);
            }
            return convertB(content, start, end);
        }
    }


    private List<byte[]> convertB(byte[] input, int start, int end) {
        List<byte[]> result = new ArrayList<>(end - start);
        //消息个数计数器
        int currentIndex = 0;
        //input index计数器
        int inputIndex = 0;
        while (currentIndex < end && inputIndex < input.length) {
            int length = findLength(input, inputIndex);
            inputIndex += 4;
            if (currentIndex >= start) {
                if (length == 0) {
                    result.add(EMPTY_ITEM);
                } else {
                    byte[] item = new byte[length];
                    for (int i = 0; i < length; i++) {
                        item[i] = input[inputIndex++];
                    }
                    result.add(item);
                }
            } else {
                inputIndex += length;
            }
            currentIndex++;
        }
        return result;
    }

    /**
     * 获取长度
     *
     * @param data   原始数据
     * @param offset 开始offset
     * @return 长度
     */
    private int findLength(byte[] data, int offset) {
        if (data.length - offset < 4) {
            return 0;
        }
        byte[] length = new byte[4];
        int lengthIndex = 0;
        for (int i = offset; i < offset + 4; i++) {
            length[lengthIndex++] = data[i];
        }
        return BytesHelper.byteArrayToInt(length);
    }

}

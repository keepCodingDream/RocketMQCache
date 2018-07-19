package io.impl;

import io.operation.AbstractFileOperation;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Map;

/**
 * @author lurenjie
 */
public class RandomMapFileOperation extends AbstractFileOperation<java.nio.ByteBuffer> {

    public RandomMapFileOperation(Map<Integer, FileChannel> fileHolder, Map<Integer, RandomAccessFile> fileOriHolder) {
        super(fileHolder, fileOriHolder);
    }

    @Override
    public long doSave(int index, long startOffset, long endOffset, java.nio.ByteBuffer data) throws IOException {
        synchronized (FILE_ORI_HOLDER.get(index)) {
            RandomAccessFile file = FILE_ORI_HOLDER.get(index);
            file.seek(startOffset);
            byte[] content = new byte[data.position()];
            data.flip();
            data.get(content);
            file.write(content, 0, content.length);
            return System.nanoTime();
        }
    }

    @Override
    public List<byte[]> doRead(String queueName,int index, long startOffset, long endOffset, int start, int end) throws IOException {
        synchronized (FILE_HOLDER.get(index)) {
            FileChannel fc = FILE_HOLDER.get(index);
            fc.position(startOffset);
            ByteBuffer buffer = ByteBuffer.allocateDirect((int) (endOffset - startOffset));
            fc.read(buffer, startOffset);
            buffer.flip();
            return convertB2BBDirM(buffer, start, end);
        }
    }
}

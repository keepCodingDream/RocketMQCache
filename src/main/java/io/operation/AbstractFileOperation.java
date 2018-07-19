package io.operation;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class AbstractFileOperation<T> implements FileOperation<T> {

    protected Map<Integer, FileChannel> FILE_HOLDER;

    protected Map<Integer, RandomAccessFile> FILE_ORI_HOLDER;

    protected static final byte[] EMPTY_ITEM = new byte[0];
    /**
     * 每页的数据量 byte
     */
    protected static final int EACH_PAGE_SIZE = 4 << 10;

    protected AbstractFileOperation(Map<Integer, FileChannel> FILE_HOLDER, Map<Integer, RandomAccessFile> FILE_ORI_HOLDER) {
        this.FILE_HOLDER = FILE_HOLDER;
        this.FILE_ORI_HOLDER = FILE_ORI_HOLDER;
    }

    public static List<byte[]> convertB2BBDirM(ByteBuffer byteBuffer, int start, int end) {
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

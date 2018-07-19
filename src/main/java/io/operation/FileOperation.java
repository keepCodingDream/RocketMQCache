package io.operation;

import java.io.IOException;
import java.util.List;

public interface FileOperation<T> {
    long doSave(int index, long startOffset, long endOffset, T data) throws IOException;

    List<byte[]> doRead(String queueName, int index, long startOffset, long endOffset, int start, int end) throws IOException;

}

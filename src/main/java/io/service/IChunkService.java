package io.service;

import java.util.List;

public interface IChunkService {

    void processData(String queueName, byte[] data) throws Exception;

    List<byte[]> queryData(String queueName, long offset, long num) throws Exception;

}

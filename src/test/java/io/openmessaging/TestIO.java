package io.openmessaging;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 测试50字节左右的写入速度
 *
 * @author tracy.
 * @create 2018-06-20 14:44
 **/
public class TestIO {
    private static Map<String, List<String>> MAP = new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {
        //System.out.println("bbfbfewfbyw128bchhwefbbbfbfew121212fbyw128bchhwefbxx:12345".getBytes().length);
        testIO();
//        List<ByteBuffer> data = new ArrayList<>(1000);
//        for (int i = 0; i < 1000; i++) {
//            ByteBuffer byteBuffer = ByteBuffer.allocate(2 << 10);
//            byteBuffer.put(new byte[2 << 10]);
//            data.add(byteBuffer);
//        }
//        long start = System.nanoTime();
//        RandomAccessFile file = new RandomAccessFile("/Users/lurenjie/data/test.log", "rw");
//        FileChannel fc = file.getChannel();
//        testMapWriteSpeed(data,fc);
        //2k---426069974  4k 390054854  8k 371856883  16k 402912990 --dir map
        //2k---10124293   4k 12468754   8k 15830930   16k 24157917  --dir seq

        //2k---8842940 3347369  4k 11094101   8k 14435289   16k 15971803  --heap seq
        //2k---415930797   4k 507033763   8k 362112556   16k 442745150  --heap map

//        testSeqWriteSpeed(data,file);
//        System.out.println(System.nanoTime() - start);
//        RandomAccessFile file = new RandomAccessFile("/Users/lurenjie/test_data/8.log", "rw");
//        FileChannel fc = file.getChannel();
//        long start = System.nanoTime();
//        testMapReadSpeed(fc);//1M--- 25058193
////        testSeqReadSpeed(file);//2M--- 8336616
//        System.out.println(System.nanoTime() - start);
    }


    public static void testMapWriteSpeed(List<ByteBuffer> data, FileChannel fc) throws Exception {
        long start = 0;
        for (ByteBuffer item : data) {
            MappedByteBuffer mbuf = fc.map(FileChannel.MapMode.READ_WRITE, start, item.position());
            mbuf.put(item);
            start += item.position();
        }
    }

    public static byte[] testMapReadSpeed(FileChannel fc) throws Exception {
        long start = 0;
        byte[] content = new byte[2 << 10];
        for (int i = 0; i < 1000; i++) {
            MappedByteBuffer mbuf = fc.map(FileChannel.MapMode.READ_WRITE, start, 2 << 10);
            mbuf.get(content);
        }
        return null;
    }

    public static void testSeqWriteSpeed(List<ByteBuffer> data, RandomAccessFile file) throws Exception {
        long startOffset = 0;
        for (ByteBuffer item : data) {
            file.seek(startOffset);
            byte[] content = new byte[item.position()];
            item.flip();
            item.get(content);
            file.write(content, 0, content.length);
        }
    }

    public static byte[] testSeqReadSpeed(RandomAccessFile file) throws Exception {
        long startOffset = 0;
        byte[] content = new byte[2 << 10];
        for (int i = 0; i < 1000; i++) {
            file.seek(startOffset);
            file.read(content);
        }
        return null;
    }


    private static void testIO() throws InterruptedException {

        long start = System.currentTimeMillis();
        DefaultQueueStoreImpl chunkServer = new DefaultQueueStoreImpl();
        //760 index==1
        Thread a = new Thread(new TestThread(chunkServer, "Queue-1"));
        Thread b = new Thread(new TestThread(chunkServer, "Queue-762"));
        a.start();
        a.join();
        b.start();
        b.join();
        System.out.println("write finish,spend:" + (System.currentTimeMillis() - start));
        long startIndex = 0;
        for (int i = 0; i < 100; i++) {
            Collection<byte[]> data = chunkServer.get("Queue-1", startIndex, 10);
            for (byte[] item : data) {
                System.out.println(new String(item));
            }
            System.out.println("total:" + data.size());
            startIndex += 10;
        }
        System.out.println("----------");


        startIndex = 0;
        for (int i = 0; i < 100; i++) {
            Collection<byte[]> data2 = chunkServer.get("Queue-762", startIndex, 10);
            for (byte[] item : data2) {
                System.out.println(new String(item));
            }
            startIndex += 10;
            System.out.println("total:" + data2.size());
        }
    }

    public static class TestThread implements Runnable {
        DefaultQueueStoreImpl chunkServer;
        String queueName;

        TestThread(DefaultQueueStoreImpl chunkServer, String queueName) {
            this.chunkServer = chunkServer;
            this.queueName = queueName;
        }

        @Override
        public void run() {
            for (int i = 0; i < 2000; i++) {
                try {
                    if (i % 3 == 0) {
                        chunkServer.put(queueName, new byte[0]);
//                        chunkServer.put(queueName, ("Queue" + String.valueOf(i)).getBytes());
                    } else {
                        chunkServer.put(queueName, ("Queue" + String.valueOf(i)).getBytes());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void testRead() throws Exception {
        QueueStore queueStore = new DefaultQueueStoreImpl();
        Collection<byte[]> collection = queueStore.get("Queue-273", 691, 10);
        for (byte[] item : collection) {
            System.out.println(new String(item));
        }
    }

    public static void testStore() throws Exception {
        QueueStore queueStore = new DefaultQueueStoreImpl();
        int size = 1000000;
        for (int i = 0; i < size; i++) {
            queueStore.put("test", ("Queue-" + i).getBytes());
        }
        System.out.println("finish");
    }

    public static void testNIO() throws IOException {
        byte[] content = new byte[50];
        byte[] content2 = new byte[50];
        for (int i = 0; i < content.length; i++) {
            content[i] = 126;
            content2[i] = 110;
        }
        long specialOffset = 0;
        RandomAccessFile acf = new RandomAccessFile("/Users/lurenjie/data/test.log", "rw");
        FileChannel fc = acf.getChannel();
        int len = content.length * 1000;
        long offset = 0;
        int i = 200000;
        long start = System.currentTimeMillis();
        while (i > 0) {
            MappedByteBuffer mbuf = fc.map(FileChannel.MapMode.READ_WRITE, offset, len);
            for (int j = 0; j < 1000; j++) {
                if (j == 234) {
                    mbuf.put(content2);
                    specialOffset = offset + 234 * 50;
                } else {
                    mbuf.put(content);
                }
            }
            offset = offset + len;
            i = i - 1000;
        }
        System.out.println(System.currentTimeMillis() - start);
        fc.close();
        acf = new RandomAccessFile("/Users/lurenjie/data/test.log", "rw");
        fc = acf.getChannel();
        byte[] input = new byte[50];
        MappedByteBuffer mbuf = fc.map(FileChannel.MapMode.READ_WRITE, specialOffset, 50);
        mbuf.get(input, 0, 50);
        for (int j = 0; j < input.length; j++) {
            System.out.println(input[j]);
        }

        fc.close();
    }
}

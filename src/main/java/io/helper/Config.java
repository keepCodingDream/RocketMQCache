package io.helper;

public class Config {
    public static String BASE_PATH = "/alidata1/race2018/data/";
    /**
     * 有多少个文件。因为不能一个queue一个文件，那样并发高了以后无法控制打开的文件句柄
     */
    public static final int TOTAL_CHUNK = 1000;

    /**
     * 支持多少个topic
     */
    public static int TOTAL_TOPIC = 1000000;

    /**
     * 每个缓存List最多缓存多少个消息
     */
    public static final int EACH_QUEUE_SIZE = 50;


    /**
     * 每页的数据量 byte
     */
    public static final int EACH_PAGE_SIZE = 3150;
}

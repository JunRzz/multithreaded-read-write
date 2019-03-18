package com.liujj.main;

import org.apache.commons.io.FileUtils;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @program: demo
 * @description:
 * @author: liujj
 * @create: 2019-03-12 17:36
 **/
public class MultiThreadReading {
    /**
     * 单个片键的大小
     */
    private static final long CHUNK_SIZE = 100 * 1024 * 1024L;
    private static ExecutorService executorService;
    /**
     * 储存的路径
     */
    private static final String PATH_PREFIX = "D:\\dataTest\\uploads\\";

    public static void main(String[] args) throws IOException {
        executorService = Executors.newFixedThreadPool(20);
        Scanner sc = new Scanner(System.in);
        System.out.print("请输入文件路径:");
        String path = sc.nextLine();
        System.out.println("你输入的路径为：" + path);
        System.out.println("开始计算MD5");
        File file = new File(path);
        //先计算MD5
        String MD5 = getFileMD5(file);
        System.out.println("MD5：" + MD5);
        //检查MD5是否存在
        boolean checkMD5 = checkMD5(MD5);
        int chunk = checkChunk(MD5, file.getName());
        //如果MD5值匹配且分片检查完成则表示文件已经上传过
        if (checkMD5 && chunk == -1) {
            System.out.println("秒传成功！！");
        } else {
            upload(MD5, file, chunk);
        }


    }

    private static void upload(String md5, File file, int chunk) {
        String dir = PATH_PREFIX + md5;
        File dirFile = new File(dir);
        if (!dirFile.exists()) {
            dirFile.mkdirs();
        }
        File uploadFile = new File(dirFile, file.getName());
        //config文件，记录分片的完成情况
        File configFile = new File(dirFile, file.getName() + ".config");
        long fileSize = file.length();
        long chunks = fileSize / CHUNK_SIZE + 1;
        System.out.println("文件将分为 " + chunks + " 片");
        FileInputStream inputStream = null;
        try {
            inputStream = new FileInputStream(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        FileChannel fileChannel = inputStream.getChannel();
        MappedByteBuffer byteBuffer = null;
        for (int i = chunk; i < chunks; i++) {
            byte[] dst;
            //声明这个值纯粹是因为在匿名内部类中引用外部变量需要是final，而有i++这种操作effective final不会生效
            int thisChunk = i;
            if (i + 1 == chunks) {
                //最后一个分片的大小，大部分情况不会是分片大小的整数倍，而最后一个片键不能简单粗暴的使用CHUNK_SIZE，会出现异常。
                long last = fileSize - i * CHUNK_SIZE;
                if (last == 0) {
                    break;
                }
                dst = new byte[(int) last];
            } else {
                dst = new byte[(int)CHUNK_SIZE];
            }
            long position = thisChunk * CHUNK_SIZE;
            try {
                byteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, position, dst.length);
                byteBuffer.get(dst);
            } catch (IOException e) {
                e.printStackTrace();
            }

            RandomAccessFile accessFile = null;
            try {
                accessFile = new RandomAccessFile(uploadFile, "rw");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            FileChannel channel = accessFile.getChannel();
            //开启多线程写入
            executorService.submit(() -> {
                MappedByteBuffer writerBuffer = null;
                try {
                    writerBuffer = channel.map(FileChannel.MapMode.READ_WRITE, position, dst.length);
                    writerBuffer.put(dst);
                    if (recordPosition(thisChunk, (int) chunks, configFile)) {
                        System.out.println("分片写入完成");

                        channel.close();
                        System.exit(0);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }finally {
                    try {
                        unMapBuffer(writerBuffer, channel.getClass());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });

        }
        try {
            unMapBuffer(byteBuffer, fileChannel.getClass());
            fileChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //检查片键是否完成写入，所有已完成则返回-1，否者返回片键编号，并从片键号开始写入
    private static int checkChunk(String MD5, String fileName) {
        File configDir = new File(PATH_PREFIX + MD5);
        File configFile = new File(configDir, fileName + ".config");
        return checkChunk(configFile);
    }

    private static int checkChunk(File configFile) {
        byte[] bytes;
        try {
            bytes = FileUtils.readFileToByteArray(configFile);
            for (int i = 0; i < bytes.length; i++) {
                if (bytes[i] != Byte.MAX_VALUE) {
                    return i;
                }
            }
        } catch (FileNotFoundException e) {
            System.out.println("配置文件不存在，重新上传");
            return 0;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }
   //在config文件中记录分片的完成情况，如果完成则写入Byte.Max_value
    private static boolean recordPosition(int chunk, int totalChunk, File configFile) {
        RandomAccessFile accessFile = null;
        try {
            accessFile = new RandomAccessFile(configFile, "rw");
            accessFile.setLength(totalChunk);
            accessFile.seek(chunk);
            accessFile.write(Byte.MAX_VALUE);
            System.out.println("分片 " + chunk + " complete");
            return checkChunk(configFile) == -1;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                accessFile.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    private static boolean checkMD5(String md5) {
        File directory = new File("D:\\dataTest\\uploads");
        if (!directory.exists()) {
            directory.mkdirs();
        }
        File[] files = directory.listFiles();
        for (File f : files) {
            if (md5.equals(f.getName())) {
                return true;
            }
        }
        return false;
    }


    public static String getFileMD5(File file) throws IOException {
        FileInputStream fileInputStream = new FileInputStream(file);
        FileChannel fileChannel = fileInputStream.getChannel();
        long remaining = file.length();
        long position = 0;
        MappedByteBuffer byteBuffer = null;

        String MD5 = null;
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            while (remaining > 0) {
                long size = Integer.MAX_VALUE / 2;
                if (size > remaining) {
                    size = remaining;
                }
                byteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, position, size);
                digest.update(byteBuffer);
                position += size;
                remaining -= size;
            }
            unMapBuffer(byteBuffer, fileChannel.getClass());
            MD5 = toHex(digest.digest());
        } catch (NoSuchAlgorithmException | IOException e) {
            e.printStackTrace();
        } finally {
            fileChannel.close();
            fileInputStream.close();
        }
        return MD5;
    }

    /**
     * JDK不提供MappedByteBuffer的释放，但是MappedByteBuffer在Full GC时才被回收，通过手动释放的方式让其回收
     *
     * @param buffer
     */
    public static void unMapBuffer(MappedByteBuffer buffer, Class channelClass) throws IOException {
        if (buffer == null) {
            return;
        }

        Throwable throwable = null;
        try {
            Method unmap = channelClass.getDeclaredMethod("unmap", MappedByteBuffer.class);
            unmap.setAccessible(true);
            unmap.invoke(channelClass, buffer);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throwable = e;
        }

        if (throwable != null) {
            throw new IOException("MappedByte buffer unmap error", throwable);
        }
    }

    private static String toHex(byte[] bytes) {

        final char[] hexDigits = "0123456789ABCDEF".toCharArray();
        StringBuilder ret = new StringBuilder(bytes.length * 2);
        for (int i = 0; i < bytes.length; i++) {
            ret.append(hexDigits[(bytes[i] >> 4) & 0x0f]);
            ret.append(hexDigits[bytes[i] & 0x0f]);
        }
        return ret.toString();
    }
}



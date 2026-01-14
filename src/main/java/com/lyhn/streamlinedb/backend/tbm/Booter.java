package com.lyhn.streamlinedb.backend.tbm;

import com.lyhn.streamlinedb.backend.common.Error;
import com.lyhn.streamlinedb.backend.utils.Panic;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

// 记录第一个表的uid
public class Booter {
    public static final String BOOTER_SUFFIX = ".bt";
    public static final String BOOTER_TMP_SUFFIX = ".bt_tmp";

    String path;
    File file;

    // 创建新的Booter
    public static Booter create(String path){
        removeBadTemp(path);
        File f = new File(path + BOOTER_SUFFIX);
        try {
            if(!f.createNewFile()){
                Panic.panic(Error.failCreateFile);
            }
        } catch (IOException e) {
            Panic.panic(e);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.noFilePermission);
        }
        return new Booter(path, f);
    }

    // 打开已存在的Booter
    public static Booter open(String path) {
        removeBadTemp(path);
        File f = new File(path+BOOTER_SUFFIX);
        if(!f.exists()) {
            Panic.panic(Error.failCreateFile);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.noFilePermission);
        }
        return new Booter(path, f);
    }

    private static void removeBadTemp(String path) {
        new File(path + BOOTER_TMP_SUFFIX).delete();
    }

    private Booter(String path, File file) {
        this.path = path;
        this.file = file;
    }

    // 从 .bt 文件中读取第一个表的UID
    public byte[] load(){
        byte[] buf = null;
        try {
            buf = Files.readAllBytes(file.toPath());
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf;
    }

    // 更新数据
    public void update(byte[] data) {
        // 创建临时文件
        File tmp = new File(path + BOOTER_TMP_SUFFIX);
        try {
            tmp.createNewFile();
        } catch (Exception e) {
            Panic.panic(e);
        }
        if(!tmp.canRead() || !tmp.canWrite()) {
            Panic.panic(Error.noFilePermission);
        }
        try(FileOutputStream out = new FileOutputStream(tmp)){
            // 将数据写入临时文件
            out.write(data);
            out.flush();
        }catch (IOException e){
            Panic.panic(e);
        }
        try{
            // 原子性替换：将临时文件替换为正式文件
            Files.move(tmp.toPath(), new File(path+BOOTER_SUFFIX).toPath(), StandardCopyOption.REPLACE_EXISTING);
        }catch (IOException e){
            Panic.panic(e);
        }
        file = new File(path+BOOTER_SUFFIX);
        if(!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.noFilePermission);
        }
    }
}

package com.lyhn.streamlinedb.backend.dm.page;

import com.lyhn.streamlinedb.backend.dm.pageCache.PageCache;
import com.lyhn.streamlinedb.backend.utils.Parser;

import java.util.Arrays;

// 普通页面
// 【空闲指针 【data】【data】】
public class PageX {
    private static final short OF_FREE = 0;
    private static final short OF_DATA = 2;

    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;

    public static byte[] InitRaw(){
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFSO(raw,OF_DATA);
        return raw;
    }

    private static void setFSO(byte[] raw, short ofData) {
        System.arraycopy(Parser.short2Byte(ofData),0,raw,OF_FREE,OF_DATA);
    }

    // 获取pg的FSO
    public static short getFSO(Page pg) {
        return getFSO(pg.getData());
    }

    // 从raw数组的前两个字节解析空闲空间偏移量
    private static short getFSO(byte[] raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2));
    }

    // 插入数据，并且返回插入位置
    public static short insert(Page pg,byte[] raw) {
        pg.setDirty(true);
        short offset = getFSO(pg.getData());
        System.arraycopy(raw,0,pg.getData(),offset,raw.length);
        // 更新空闲空间偏移量
        setFSO(pg.getData(),(short)(offset + raw.length));
        return offset;
    }

    // 将raw插入pg中的offset位置，并将pg.data的offset设置为较大的offset
    public static void recoverInsert(Page pg,byte[] raw,short offset) {
        pg.setDirty(true);
        System.arraycopy(raw,0,pg.getData(),offset,raw.length);

        // 更新空闲空间偏移量
        short rawFSO = getFSO(pg.getData());
        if(rawFSO < offset + raw.length){
            setFSO(pg.getData(),(short)(offset + raw.length));
        }
    }

    public static void recoverUpdate(Page pg,byte[] raw,short offset){
        pg.setDirty(true);
        System.arraycopy(raw,0,pg.getData(),offset,raw.length);
    }
}

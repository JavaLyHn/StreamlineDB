package com.lyhn.streamlinedb.backend.dm.dataItem;

import com.lyhn.streamlinedb.backend.common.SubArray;
import com.lyhn.streamlinedb.backend.dm.DataManagerImpl;
import com.lyhn.streamlinedb.backend.dm.page.Page;
import com.lyhn.streamlinedb.backend.utils.Parser;
import com.lyhn.streamlinedb.backend.utils.Types;
import com.lyhn.streamlinedb.backend.dm.DataManager;

import java.util.Arrays;

public interface DataItem {
    SubArray data();

    void before();
    void unBefore();
    void after(long xid);
    void release();

    void lock();
    void unlock();
    void rLock();
    void rUnLock();

    Page page();
    long getUid();
    byte[] getOldRaw();
    SubArray getRaw();

    public static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short) raw.length);
        byte[] result = new byte[1 + 2 + raw.length];
        System.arraycopy(valid,0,result,0,1);
        System.arraycopy(size,0,result,1,2);
        System.arraycopy(raw,0,result,3,raw.length);
        return result;
    }

    // 从页面的offset处解析处dataitem
    public static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm) {
        byte[] raw = pg.getData();
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset+DataItemImpl.OF_SIZE, offset+DataItemImpl.OF_DATA));
        short length = (short)(size + DataItemImpl.OF_DATA);
        long uid = Types.addressToUid(pg.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(raw, offset, offset+length), new byte[length], pg, uid, dm);
    }

    public static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte)1;
    }
}

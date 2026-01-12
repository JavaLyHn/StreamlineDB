package com.lyhn.streamlinedb.backend.dm;

import com.lyhn.streamlinedb.backend.common.AbstractCache;
import com.lyhn.streamlinedb.backend.common.Error;
import com.lyhn.streamlinedb.backend.dm.dataItem.DataItem;
import com.lyhn.streamlinedb.backend.dm.dataItem.DataItemImpl;
import com.lyhn.streamlinedb.backend.dm.logger.Logger;
import com.lyhn.streamlinedb.backend.dm.page.Page;
import com.lyhn.streamlinedb.backend.dm.page.PageOne;
import com.lyhn.streamlinedb.backend.dm.page.PageX;
import com.lyhn.streamlinedb.backend.dm.pageCache.PageCache;
import com.lyhn.streamlinedb.backend.dm.pageIndex.PageIndex;
import com.lyhn.streamlinedb.backend.dm.pageIndex.PageInfo;
import com.lyhn.streamlinedb.backend.tm.TransactionManager;
import com.lyhn.streamlinedb.backend.utils.Panic;
import com.lyhn.streamlinedb.backend.utils.Types;

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager{
    // 事务管理器
    TransactionManager tm;
    // 页面缓存
    PageCache pc;
    // 日志记录器
    Logger logger;
    // 页面索引
    PageIndex pIndex;
    // 第一页，用于判断数据库是否正常关闭
    Page pageOne;

    public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
        super(0);
        this.pc = pc;
        this.logger = logger;
        this.tm = tm;
        this.pIndex = new PageIndex();
    }

    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl)super.get(uid);
        if(!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }

    // 插入新数据
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        // 将数据包装成DataItem格式
        byte[] raw = DataItem.wrapDataItemRaw(data);
        // 检查页面长度是否超过页面的最大空闲空间
        if(raw.length > PageX.MAX_FREE_SPACE) {
            throw Error.dataTooLargeException;
        }

        // 尝试从页面索引中选择一个有足够空闲空间的页面，如果没有则创建新页面。
        PageInfo pi = null;
        for(int i = 0; i < 5; i ++) {
            pi = pIndex.select(raw.length);
            if (pi != null) {
                break;
            } else {
                int newPgno = pc.newPage(PageX.InitRaw());
                pIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }

        // 如果尝试 5 次后仍未找到合适的页面，则抛出数据库繁忙异常
        if(pi == null) {
            throw Error.databaseBusyException;
        }

        Page pg = null;
        int freeSpace = 0;
        try {
            // 获取选中的页面
            pg = pc.getPage(pi.pgno);

            // 生成插入日志并记录
            byte[] log = Recover.insertLog(xid, pg, raw);
            logger.log(log);

            // 将数据插入页面
            short offset = PageX.insert(pg, raw);

            // 释放页面
            pg.release();
            // 返回插入数据的唯一标识符（uid）
            return Types.addressToUid(pi.pgno, offset);
        } finally {
            // 将取出的pg重新插入pIndex
            // 更新页面索引中该页面的空闲空间信息
            if(pg != null) {
                pIndex.add(pi.pgno, PageX.getFreeSpace(pg));
            } else {
                pIndex.add(pi.pgno, freeSpace);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        logger.close();

        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }

    // 当第一次通过 uid 查询数据时，会先根据 uid 找到 数据页 page，然后将 page 加入缓存中，然后根据 uid 在 page 中找到数据项 DateItem
    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pgno = (int)(uid & ((1L << 32) - 1));
        Page pg = pc.getPage(pgno);
        return DataItem.parseDataItem(pg, offset, this);
    }

    @Override
    protected void releaseForCache(DataItem di) {
        di.page().release();
    }

    // 为xid生成update日志
    public void logDataItem(long xid, DataItemImpl dataItem) {
        // 将数据项的更新操作转换为日志格式
        byte[] log = Recover.updateLog(xid, dataItem);
        logger.log(log);
    }

    public void releaseDataItem(DataItemImpl dataItem) {
        super.release(dataItem.getUid());
    }

    // 在打开已有文件时时读入PageOne，并验证正确性
    boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    // 在创建文件时初始化PageOne
    public void initPageOne() {
        int pgno = pc.newPage(PageOne.InitRaw());
        assert pgno == 1;
        try {
            pageOne = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(pageOne);
    }

    // 初始化pageIndex
    void fillPageIndex() {
        int pageNumber = pc.getPageNumber();
        for(int i = 2; i <= pageNumber; i ++) {
            Page pg = null;
            try {
                pg = pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            pg.release();
        }
    }
}

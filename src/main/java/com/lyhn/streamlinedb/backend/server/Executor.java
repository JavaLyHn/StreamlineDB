package com.lyhn.streamlinedb.backend.server;

import com.lyhn.streamlinedb.backend.parser.Parser;
import com.lyhn.streamlinedb.backend.parser.statement.*;
import com.lyhn.streamlinedb.backend.tbm.BeginRes;
import com.lyhn.streamlinedb.backend.tbm.TableManager;
import com.lyhn.streamlinedb.backend.common.Error;
public class Executor {
    private long xid;
    TableManager tbm;

    public Executor(TableManager tbm) {
        this.tbm = tbm;
        this.xid = 0;
    }

    public void close() {
        if(xid != 0) {
            System.out.println("Abnormal Abort: " + xid);
            tbm.abort(xid);
        }
    }

    public byte[] execute(byte[] sql) throws Exception {
        System.out.println("Execute: " + new String(sql));
        // 解析SQL语句，得到语句对象
        Object stat = Parser.Parse(sql);
        // 判断stat是不是Begin类或者其子类
        if(Begin.class.isInstance(stat)) {
            if(xid != 0) {
                throw Error.nestedTransactionException;
            }
            // 开始事务
            BeginRes r = tbm.begin((Begin)stat);
            xid = r.xid;
            return r.result;
        } else if(Commit.class.isInstance(stat)) {
            if(xid == 0) {
                throw Error.noTransactionException;
            }
            // 提交事务
            byte[] res = tbm.commit(xid);
            xid = 0;
            return res;
        } else if(Abort.class.isInstance(stat)) {
            if(xid == 0) {
                throw Error.noTransactionException;
            }
            // 回滚事务
            byte[] res = tbm.abort(xid);
            xid = 0;
            return res;
        } else {
            // 其他SQL操作
            return execute2(stat);
        }
    }

    private byte[] execute2(Object stat) throws Exception {
        boolean tmpTransaction = false;
        Exception e = null;
        if(xid == 0) {
            // xid为0表示没有活跃事务
            tmpTransaction = true;
            // 默认隔离级别为读已提交
            BeginRes r = tbm.begin(new Begin());
            // 创建一个临时事务
            xid = r.xid;
        }
        try {
            byte[] res = null;
            if(Show.class.isInstance(stat)) {
                res = tbm.show(xid);
            } else if(Create.class.isInstance(stat)) {
                res = tbm.create(xid, (Create)stat);
            } else if(Select.class.isInstance(stat)) {
                res = tbm.read(xid, (Select)stat);
            } else if(Insert.class.isInstance(stat)) {
                res = tbm.insert(xid, (Insert)stat);
            } else if(Delete.class.isInstance(stat)) {
                res = tbm.delete(xid, (Delete)stat);
            } else if(Update.class.isInstance(stat)) {
                res = tbm.update(xid, (Update)stat);
            }
            return res;
        } catch(Exception e1) {
            e = e1;
            throw e;
        } finally {
            if(tmpTransaction) {
                if(e != null) {
                    tbm.abort(xid);
                } else {
                    tbm.commit(xid);
                }
                xid = 0;
            }
        }
    }
}

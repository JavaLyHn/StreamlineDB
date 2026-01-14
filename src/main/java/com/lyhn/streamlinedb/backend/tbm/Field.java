package com.lyhn.streamlinedb.backend.tbm;

import com.google.common.primitives.Bytes;
import com.lyhn.streamlinedb.backend.common.Error;
import com.lyhn.streamlinedb.backend.im.BPlusTree;
import com.lyhn.streamlinedb.backend.parser.statement.SingleExpression;
import com.lyhn.streamlinedb.backend.tm.TransactionManagerImpl;
import com.lyhn.streamlinedb.backend.utils.Panic;
import com.lyhn.streamlinedb.backend.utils.ParseStringRes;
import com.lyhn.streamlinedb.backend.utils.Parser;

import java.util.Arrays;
import java.util.List;

/**
 * field 表示数据表中的字段信息
 * 二进制格式为：
 * [FieldName][TypeName][IndexUid]
 * 如果field无索引，IndexUid为0
 */
public class Field {
    long uid;
    private Table tb;
    // 字段名
    String fieldName;
    // 字段类型
    String fieldType;
    // 索引信息
    private long index;
    // 索引树
    private BPlusTree bt;

    // 加载字段
    public static Field loadField(Table tb, long uid) {
        byte[] raw = null;
        try {
            raw = ((TableManagerImpl)tb.tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;
        return new Field(uid, tb).parseSelf(raw);
    }

    public Field(long uid, Table tb) {
        this.uid = uid;
        this.tb = tb;
    }

    public Field(Table tb, String fieldName, String fieldType, long index) {
        this.tb = tb;
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.index = index;
    }

    // 给字段的filedName、filedType、index赋值
    private Field parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes res = Parser.parseString(raw);
        // 解析字段名
        fieldName = res.str;
        position += res.next;
        res = Parser.parseString(Arrays.copyOfRange(raw, position, raw.length));
        // 解析字段类型
        fieldType = res.str;
        position += res.next;
        // 解析索引uid
        this.index = Parser.parseLong(Arrays.copyOfRange(raw, position, position+8));
        if(index != 0) {
            try {
                // 如果有索引，加载B+树索引
                bt = BPlusTree.load(index, ((TableManagerImpl)tb.tbm).dm);
            } catch(Exception e) {
                Panic.panic(e);
            }
        }
        return this;
    }

    // 创建字段
    public static Field createField(Table tb, long xid, String fieldName, String fieldType, boolean indexed) throws Exception {
        typeCheck(fieldType);
        Field f = new Field(tb, fieldName, fieldType, 0);
        // 需要索引
        if(indexed) {
            // 创建B+树索引，并返回uid
            long index = BPlusTree.create(((TableManagerImpl)tb.tbm).dm);
            // 加载B+树到内存
            BPlusTree bt = BPlusTree.load(index, ((TableManagerImpl)tb.tbm).dm);
            // 设置索引uid
            f.index = index;
            // 设置B+树引用
            f.bt = bt;
        }
        // 持久化保存字段信息
        f.persistSelf(xid);
        return f;
    }

    // 将字段信息持久化到数据库
    private void persistSelf(long xid) throws Exception {
        // 将字段信息转为字节数组
        byte[] nameRaw = Parser.string2Byte(fieldName);
        // 将字段类型转为字节数组
        byte[] typeRaw = Parser.string2Byte(fieldType);
        // 将索引uid转为字节数组
        byte[] indexRaw = Parser.long2Byte(index);
        // 通过VersionManager插入字段信息，并返回uid
        this.uid = ((TableManagerImpl)tb.tbm).vm.insert(xid, Bytes.concat(nameRaw, typeRaw, indexRaw));
    }

    private static void typeCheck(String fieldType) throws Exception {
        if(!"int32".equals(fieldType) && !"int64".equals(fieldType) && !"string".equals(fieldType)) {
            throw Error.invalidFieldException;
        }
    }

    public boolean isIndexed() {
        return index != 0;
    }

    public void insert(Object key, long uid) throws Exception {
        long uKey = value2Uid(key);
        bt.insert(uKey, uid);
    }

    public List<Long> search(long left, long right) throws Exception {
        return bt.searchRange(left, right);
    }

    /**
     *   - value2Uid(1) → 1 (int32)
     *   - value2Uid(100L) → 100 (int64)
     *   - value2Uid("abc") → 97 * 13331² + 98 * 13331 + 99 (哈希值)
     */
    public long value2Uid(Object key) {
        long uid = 0;
        switch(fieldType) {
            case "string":
                uid = Parser.str2Uid((String)key);
                break;
            case "int32":
                int uint = (int)key;
                return (long)uint;
            case "int64":
                uid = (long)key;
                break;
        }
        return uid;
    }

    public Object string2Value(String str) {
        switch(fieldType) {
            case "int32":
                return Integer.parseInt(str);
            case "int64":
                return Long.parseLong(str);
            case "string":
                return str;
        }
        return null;
    }

    public byte[] value2Raw(Object v) {
        byte[] raw = null;
        switch(fieldType) {
            case "int32":
                raw = Parser.int2Byte((int)v);
                break;
            case "int64":
                raw = Parser.long2Byte((long)v);
                break;
            case "string":
                raw = Parser.string2Byte((String)v);
                break;
        }
        return raw;
    }

    class ParseValueRes {
        Object v;
        int shift;
    }

    // 解析字节数组为值
    public ParseValueRes parserValue(byte[] raw) {
        ParseValueRes res = new ParseValueRes();
        switch(fieldType) {
            case "int32":
                res.v = Parser.parseInt(Arrays.copyOf(raw, 4));
                res.shift = 4;
                break;
            case "int64":
                res.v = Parser.parseLong(Arrays.copyOf(raw, 8));
                res.shift = 8;
                break;
            case "string":
                ParseStringRes r = Parser.parseString(raw);
                res.v = r.str;
                res.shift = r.next;
                break;
        }
        return res;
    }

    public String printValue(Object v) {
        String str = null;
        switch(fieldType) {
            case "int32":
                str = String.valueOf((int)v);
                break;
            case "int64":
                str = String.valueOf((long)v);
                break;
            case "string":
                str = (String)v;
                break;
        }
        return str;
    }

    @Override
    public String toString() {
        return new StringBuilder("(")
                .append(fieldName)
                .append(", ")
                .append(fieldType)
                .append(index!=0?", Index":", NoIndex")
                .append(")")
                .toString();
    }

    // 计算表达式
    public FieldCalRes calExp(SingleExpression exp) throws Exception {
        Object v = null;
        FieldCalRes res = new FieldCalRes();
        switch(exp.compareOp) {
            case "<":
                res.left = 0;
                v = string2Value(exp.value);
                res.right = value2Uid(v);
                if(res.right > 0) {
                    // where id < 5 -> [0,4]所以要-1
                    res.right --;
                }
                break;
            case "=":
                // 将sql中的字符串转换为对应类型的java对象
                v = string2Value(exp.value);
                // 将java对象转换为B+树索引的uid
                res.left = value2Uid(v);
                res.right = res.left;
                break;
            case ">":
                res.right = Long.MAX_VALUE;
                v = string2Value(exp.value);
                // where id > 5 -> [6,MAX]所以要+1
                res.left = value2Uid(v) + 1;
                break;
        }
        return res;
    }
}

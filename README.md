# StreamlineDB
StreamlineDB是一个模拟MySQL的轻量级关系型数据库，采用C/S架构，支持多客户端通过Socket连接到服务器，执行SQL语句并返回查询结果。系统实现了事务管理、数据存储、日志管理、MVCC、索引构建等功能


# StreamlineDB使用方法
当前项目开发的JDK版本为JDK17,请更改项目的编译版本以适应你的JDK。
首先执行以下命令编译源码：
```
mvn compile
```
接着执行以下命令，在E:\Temp路径下创建数据库：
```
mkdir E:\Temp
```
**PowerShell写法**
```
mvn exec:java "-Dexec.mainClass=com.lyhn.streamlinedb.backend.Launcher" "-Dexec.args=-create E:\Temp\StreamlineDB"
```
**CMD写法**
```
mvn exec:java -Dexec.mainClass=com.lyhn.streamlinedb.backend.Launcher -Dexec.args="-create E:\Temp\StreamlineDB"
```
接着执行以下命令，以默认参数打开数据库，并在当前命令框内开启Server端服务：

**PowerShell写法**
```
mvn exec:java "-Dexec.mainClass=com.lyhn.streamlinedb.backend.Launcher" "-Dexec.args=-open E:\Temp\StreamlineDB"
```
**CMD写法**
```
mvn exec:java -Dexec.mainClass=com.lyhn.streamlinedb.backend.Launcher -Dexec.args="-open E:\Temp\StreamlineDB"
```
最后，在新的命令框中，执行下调命令，开启Client端服务：

**PowerShell写法**
```
mvn exec:java "-Dexec.mainClass=com.lyhn.streamlinedb.client.Launcher"
```
**CMD写法**
```
mvn exec:java -Dexec.mainClass=com.lyhn.streamlinedb.client.Launcher
```
现在，Server端服务和Client端都已经执行成功，可以开始StreamlineDB的使用了🙂🙂
下面给一个示范例子🙂🙂

0、创建一个学生表
```
create table students id int32, name string, age int32 (index id)
```
<img width="756" height="48" alt="image" src="https://github.com/user-attachments/assets/82013014-553f-4825-bb54-d7f38619bb32" />

1、显示所有表
```
show
```
<img width="717" height="55" alt="image" src="https://github.com/user-attachments/assets/fc889c8a-445f-4f15-8af1-7505e7576ec6" />

2、插入数据
```
insert into students values 1 Alice 20
insert into students values 2 Bob 21
insert into students values 3 Charlie 22
insert into students values 4 David 23
insert into students values 5 Eve 24
```
<img width="516" height="209" alt="image" src="https://github.com/user-attachments/assets/ee4431b7-2494-4fc6-89f9-2ae08a5a9547" />

3、 查询所有数据
```
select * from students
```
<img width="351" height="128" alt="image" src="https://github.com/user-attachments/assets/3bb5466a-ced9-4caa-8f55-515467fc59a1" />

4、条件查询
```
select * from students where id < 3
```
<img width="478" height="77" alt="image" src="https://github.com/user-attachments/assets/d8104b93-576d-44dc-8349-61b42b2e059b" />

5、更新数据
```
update students set age = 25 where id = 1
select * from students where id = 1
```
<img width="530" height="92" alt="image" src="https://github.com/user-attachments/assets/03601c96-91f4-41b8-accd-994fef9064fb" />

6、删除数据
```
delete from students where id = 5
select * from students
```
<img width="453" height="153" alt="image" src="https://github.com/user-attachments/assets/557c3b7f-2cc6-46c0-8afc-85927973e382" />

7、事务测试 - 提交
```
begin
insert into students values 6 Frank 26
commit
select * from students where id = 6
```
<img width="497" height="181" alt="image" src="https://github.com/user-attachments/assets/ef4cedba-cc78-40e0-a768-61da3ea604e6" />

8、事务测试 - 回滚
```
begin
insert into students values 7 Grace 27
abort
select * from students where id = 7
```
<img width="496" height="156" alt="image" src="https://github.com/user-attachments/assets/f5b82909-ef85-4bd4-9913-5271528200d0" />

9、删除表**TODO**
```
drop table students
```

10、显示所有表
```
show
```
<img width="196" height="59" alt="image" src="https://github.com/user-attachments/assets/d5089cad-86d8-4be3-95ca-2890356d2bb0" />

11、退出客户端
```
exit
```

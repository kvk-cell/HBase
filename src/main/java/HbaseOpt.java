import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class HbaseOpt {
    private static Configuration configuration;
    private static Connection connection;
    private static Admin admin;

    /**
     * 创建连接connection
     * @throws IOException
     */
    public static void getConnection() throws IOException{
        //1.获得Configuration实例并进行相关设置
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","localhost");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");

        try {
            connection = ConnectionFactory.createConnection(configuration);
            //获得Admin接口
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭连接
     * @throws IOException
     */
     public static void closeConnection() throws IOException{
        try{
            if(admin != null) {
                admin.close();
            }
            if (connection != null) {
                connection.close();
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建表
     * @param tableName 表名
     * @param familyNames 列族名
     * */
    public static void createTable(String tableName, String familyNames[]) throws IOException {
        //如果表存在退出
        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("Table："+ tableName+" exists!");
            return;
        }
        //通过HTableDescriptor类来描述一个表，HColumnDescriptor描述一个列族
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String familyName : familyNames) {
            tableDescriptor.addFamily(new HColumnDescriptor(familyName));
        }
        //tableDescriptor.addFamily(new HColumnDescriptor(familyName));
        admin.createTable(tableDescriptor);
        System.out.println("create table:"+tableName+" success!");
    }

    /**
     * 删除表
     * @param tableName 表名
     * */
    public static void dropTable(String tableName) throws IOException {
        //如果表不存在报异常
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println(tableName+"不存在");
            return;
        }

        //删除之前要将表disable
        if (!admin.isTableDisabled(TableName.valueOf(tableName))) {
            admin.disableTable(TableName.valueOf(tableName));
        }
        admin.deleteTable(TableName.valueOf(tableName));
        System.out.println("delete table " + tableName + " ok.");
    }

    /**
     * 指定单元格中插入数据
     * @param tableName 表名
     * @param rowKey 行键
     * @param family 列族
     * @param column 列
     * @param value 值
     * @throws IOException
     */
    public static void insert(String tableName, String rowKey, String family, String column, String value) throws IOException {
        //获得Table接口,需要传入表名
        Table table =connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
    }

    /**
     * 删除表中的指定单元格
     * @param tableName 表名
     * @param rowKey 行键
     * @param family 列族名
     * @param column 列
     */
    public static void delete(String tableName, String rowKey, String family, String column) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumns(Bytes.toBytes(family), Bytes.toBytes(column));
        table.delete(delete);
    }

    /**
     * 删除表中指定行键的指定列族
     * @param tableName 表名
     * @param rowKey 行键
     * @param family 列族名
     */
    public static void delete(String tableName, String rowKey, String family) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addFamily(Bytes.toBytes(family));
        table.delete(delete);
    }

    /**
     * 添加新的列族
     * @param tableName 表名
     * @param family 列族名
     * @throws IOException
     */
    public static void  alter(String tableName, String family) throws IOException {

        HTableDescriptor tableDescriptor =  admin.getTableDescriptor(TableName.valueOf(tableName));
        tableDescriptor.addFamily(new HColumnDescriptor(family));
        admin.modifyTable(TableName.valueOf(tableName), tableDescriptor);
        System.out.println("Add column family:"+family+" success!");
    }

    /**
     * 显示全表
     * @param tableName 表名
     * @throws IOException
     */
    public static void scan(String tableName) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan s = new Scan();
        ResultScanner result= table.getScanner(s);

        System.out.println("ROW         COLUMN+CELL");//表头
        for (Result r : result) {
            String row = new String(r.getRow()); //获取row key
            List<Cell> cells = r.listCells(); //将cell的内容放到list中
            //输出格式：行键     列族名:列, value值
            for (Cell c:cells) {
                System.out.println(row+"\t\t"+new String(CellUtil.cloneFamily(c))+":"+
                        new String(CellUtil.cloneQualifier(c))+", value="+new String(CellUtil.cloneValue(c)));
            }
        }
    }

    /**
     * 按列查询
     * @param tableName 表名
     * @param family 列族名
     * @throws IOException
     */
    public static void scan(String tableName, String family, String column) throws IOException {

        Table table=connection.getTable(TableName.valueOf(tableName));
        ResultScanner result = table.getScanner(family.getBytes(), column.getBytes());

        System.out.println("ROW         COLUMN+CELL");//表头
        for (Result r : result) {
            String row = new String(r.getRow());
            List<Cell> cells = r.listCells();
            for (Cell c:cells) {
                System.out.println(row+"\t\t"+new String(CellUtil.cloneFamily(c))+":"+
                        new String(CellUtil.cloneQualifier(c))+", value="+new String(CellUtil.cloneValue(c)));
            }
        }
    }

    /**
     * 按行键查询
     * @param tableName 表名
     * @param rowkey 行键
     * @throws IOException
     */
    public static void scan(String tableName, String rowkey) throws IOException {

        Table table=connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowkey.getBytes());
        Result result = table.get(get);

        System.out.println("ROW         COLUMN+CELL");//表头
        String row = new String(result.getRow());
        List<Cell> cells = result.listCells();
        for (Cell c:cells) {
            System.out.println(row+"\t\t"+new String(CellUtil.cloneFamily(c))+":"+
                    new String(CellUtil.cloneQualifier(c))+", value="+new String(CellUtil.cloneValue(c)));
        }
    }

    /**
     * 列出现有表
     * @throws IOException
     */
    public static void list() throws IOException {
        HTableDescriptor[] tableDescriptor =admin.listTables();
        if(tableDescriptor.length == 0){
            System.out.println("No Table Exist.");
        }
        else{
            for (int i=0; i<tableDescriptor.length; i++ ){
                System.out.println("Table - "+tableDescriptor[i].getNameAsString());
            }
        }
    }

    public static void main(String[] args) throws IOException {

        /**
         * 建立连接
         * */
        getConnection();

        /**
         * 1.创建表"studentInfo"并添加信息
         * */
        System.out.println("========== 1. Build table: studentInfo ==========");
        String[] familyNames= new String[] {"Description", "Course1", "Course2", "Course3"};
        createTable("studentInfo",familyNames);
        System.out.println("Add information to Table: studentInfo ...");
        //添加学生信息
        insert(  "studentInfo","2015001","Description","S_Name", "Li Lei");
        insert(  "studentInfo","2015001","Description","S_Sex", "male");
        insert(  "studentInfo","2015001","Description","S_Age", "23");
        insert(  "studentInfo","2015002","Description","S_Name", "Han Meimei");
        insert(  "studentInfo","2015002","Description","S_Sex", "female");
        insert(  "studentInfo","2015002","Description","S_Age", "22");
        insert(  "studentInfo","2015003","Description","S_Name", "Zhang San");
        insert(  "studentInfo","2015003","Description","S_Sex", "male");
        insert(  "studentInfo","2015003","Description","S_Age", "24");
        //添加学生的课程信息
        insert(  "studentInfo","2015001","Course1","C_No", "123001");
        insert(  "studentInfo","2015001","Course1","C_Name", "Math");
        insert(  "studentInfo","2015001","Course1","C_Credit", "2.0");
        insert(  "studentInfo","2015001","Course1","C_Score", "86");
        insert(  "studentInfo","2015003","Course1","C_No", "123001");
        insert(  "studentInfo","2015003","Course1","C_Name", "Math");
        insert(  "studentInfo","2015003","Course1","C_Credit", "2.0");
        insert(  "studentInfo","2015003","Course1","C_Score", "98");
        insert(  "studentInfo","2015002","Course2","C_No", "123002");
        insert(  "studentInfo","2015002","Course2","C_Name", "Computer Science");
        insert(  "studentInfo","2015002","Course2","C_Credit", "5.0");
        insert(  "studentInfo","2015002","Course2","C_Score", "77");
        insert(  "studentInfo","2015003","Course2","C_No", "123002");
        insert(  "studentInfo","2015003","Course2","C_Name", "Computer Science");
        insert(  "studentInfo","2015003","Course2","C_Credit", "5.0");
        insert(  "studentInfo","2015003","Course2","C_Score", "95");
        insert(  "studentInfo","2015001","Course3","C_No", "123003");
        insert(  "studentInfo","2015001","Course3","C_Name", "English");
        insert(  "studentInfo","2015001","Course3","C_Credit", "3.0");
        insert(  "studentInfo","2015001","Course3","C_Score", "69");
        insert(  "studentInfo","2015002","Course3","C_No", "123003");
        insert(  "studentInfo","2015002","Course3","C_Name", "English");
        insert(  "studentInfo","2015002","Course3","C_Credit", "3.0");
        insert(  "studentInfo","2015002","Course3","C_Score", "99");
        System.out.println("---------- Result: Scan studentInfo ----------");
        scan("studentInfo");


        /**
         * 2.查询选修Computer Science的学生的成绩
         * */
        System.out.println("\n\n========== 2. Query the score of students taking Computer Science ==========");
        System.out.println("Step 1：Confirm which column family of Course is Computer Science");
        scan("studentInfo", "Course1", "C_Name");
        scan("studentInfo", "Course2", "C_Name");
        scan("studentInfo", "Course3", "C_Name");
        System.out.println("Computer Science is Course2");
        System.out.println("\nStep 2：According to Step 1, scan Course2:C_Score");
        System.out.println("---------- Result: Score of Computer Science ----------");
        scan("studentInfo", "Course2", "C_Score");


        /**
         * 3.增加新的列族和新列Contact:Email，并添加数据
         * */
        System.out.println("\n\n========== 3. Add Contact:Email ==========");
        alter("studentInfo", "Contact");
        System.out.println("insert new info Contact:Email to studentInfo...");
        insert("studentInfo", "2015001", "Contact", "Email", "lilei@qq.com");
        insert("studentInfo", "2015002", "Contact", "Email", "hmm@qq.com");
        insert("studentInfo", "2015003", "Contact", "Email", "zs@qq.com");
        System.out.println("---------- Result: scan column Contact:Email ----------");
        scan("studentInfo", "Contact", "Email");


        /**
         * 4.删除学号为2015003的学生的选课记录
         * */
        System.out.println("\n\n========== 4. Delete course selection record of 2015003 ==========");
        //直接删除表中指定行键的指定列族
        System.out.println("Directly delete the specified row key:column family");
        delete("studentInfo", "2015003", "Course1");
        delete("studentInfo", "2015003", "Course2");
        delete("studentInfo", "2015003", "Course3");
//        //逐个删除单元格
//        System.out.println("Delete related cells one by one");
//        delete("studentInfo", "2015003","Course1", "C_No");
//        delete("studentInfo", "2015003","Course1", "C_Name");
//        delete("studentInfo", "2015003","Course1", "C_Credit");
//        delete("studentInfo", "2015003","Course1", "C_Score");
//        delete("studentInfo", "2015003","Course2", "C_No");
//        delete("studentInfo", "2015003","Course2", "C_Name");
//        delete("studentInfo", "2015003","Course2", "C_Credit");
//        delete("studentInfo", "2015003","Course2", "C_Score");
//        delete("studentInfo", "2015003","Course3", "C_No");
//        delete("studentInfo", "2015003","Course3", "C_Name");
//        delete("studentInfo", "2015003","Course3", "C_Credit");
//        delete("studentInfo", "2015003","Course3", "C_Score");
        System.out.println("---------- Result: scan studentInfo by rowkey 2015003 ----------");
        scan("studentInfo","2015003");


        /**
         * 5. 删除所创建的表
         * */
        System.out.println("\n\n========== 5. Drop Table:studentInfo ==========");
        System.out.println("list current tables:");
        list(); //查询现在所有的表
        System.out.println("Delete Table:studentInfo...");
        dropTable("studentInfo"); //删除表
        System.out.println("---------- Result: list current tables ----------");
        list(); //查询所有表，验证删除是否成功

        /**
         * 关闭连接
         * */
        closeConnection();
    }
}





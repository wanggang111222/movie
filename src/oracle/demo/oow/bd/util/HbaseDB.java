package oracle.demo.oow.bd.util;

import java.io.IOException;

import oracle.demo.oow.hb.dao.ConstantsHBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseDB {
	private  Configuration conf;
	private  Connection conn;
	private Table table;
	private static HbaseDB hbaseDB=null;
	
	/*private static class HbaseDBInstance{
		private static final HbaseDB instance = new HbaseDB();
	}*/
	/*private static class HbaseDBInstance{
		private static final HbaseDB HBASE_DB = new HbaseDB();
	}*/
	public static HbaseDB getInstance()
	{
		if(hbaseDB==null)
		{
			hbaseDB=new HbaseDB();
		}
		return hbaseDB;
	}
	private HbaseDB()
	{
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "admin");
		conf.set("hbase.rootdir", "hdfs://admin:9000/hbase");
		try {
			conn = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}	
	}
	public Table getTable(String tableName)
	{
		try {
			 table = conn.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return table;
	}
	
	/**
     * hbase创建表
     * @param tableName
     */
	public void createTable(String tableName,String columFamilys[])
	{
		System.out.println("start create table.....");
		deleteTable(tableName);
		try {
			Table table = conn.getTable(TableName.valueOf(tableName));
			Admin admin = conn.getAdmin();
			
			HTableDescriptor  descriptor = new HTableDescriptor(TableName.valueOf(tableName));
			for(String s:columFamilys)
			{
				HColumnDescriptor family = new HColumnDescriptor(Bytes.toBytes(s));
				descriptor.addFamily(family);
			}
			admin.createTable(descriptor);
			table.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("stop create table.....");
	}
	/**
	 * hbase删除表
	 * @param tableName
	 */
	public void deleteTable(String tableName)
	{
		System.out.println("start delete table.....");
		try {
			Admin admin = conn.getAdmin();
			if(admin.tableExists(TableName.valueOf(tableName)))
			{
				admin.disableTable(TableName.valueOf(tableName));
				admin.deleteTable(TableName.valueOf(tableName));
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("stop delete table.....");
	}
	public void close()
	{
		try {
			conn.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public long getId(String tableName, String family,
			String qualifier) {
        Table table = getTable(tableName);
        long id = 0l;
        try {
			id=table.incrementColumnValue(Bytes.toBytes(ConstantsHBase.ROW_KEY_GID_ACTIVITY_ID),
					Bytes.toBytes(family),
					Bytes.toBytes(qualifier),
					1);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return id;
	}

}

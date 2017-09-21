package oracle.demo.oow.bd.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import oracle.kv.impl.api.table.query.TableParser.Binary_defContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class HbaseUtil {
	private static Configuration conf;
	private static Connection conn;

	public static void init() {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "admin");
		conf.set("hbase.rootdir", "hdfs://admin:9000/hbase");
		try {
			conn = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public static void put(String tableName, String rowKey,
			List<TableInfo> tableInfoList) {
		try {
			init();
			Table table = conn.getTable(TableName.valueOf(tableName));
			Put put = new Put(Bytes.toBytes(rowKey));
			for (TableInfo tableInfo : tableInfoList) {
				/*if(tableInfo.getValue()==null)
				{
					System.out.println(11);
					put.addColumn(Bytes.toBytes(tableInfo.getFamily()),
							Bytes.toBytes(tableInfo.getQualifier()),
							Bytes.toBytes(tableInfo.getValue1()));
				}
				else{
					System.out.println(22);
					put.addColumn(Bytes.toBytes(tableInfo.getFamily()),
							Bytes.toBytes(tableInfo.getQualifier()),
							Bytes.toBytes(tableInfo.getValue()));
				}*/
				put.addColumn(Bytes.toBytes(tableInfo.getFamily()),
						Bytes.toBytes(tableInfo.getQualifier()),
						Bytes.toBytes(tableInfo.getValue()));
				
			}
			table.put(put);
			table.close();
			end();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static List<Result> getAll(String tableName)
	{
		init();
		List<Result> results = new ArrayList<Result>();
		
		try {
			Table table=conn.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
			ResultScanner resultScanner = table.getScanner(scan);
			Iterator<Result> iter =resultScanner.iterator();
			while(iter.hasNext())
			{
				Result result = iter.next();
				results.add(result);
				System.out.println(result);
			}
			table.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		end();
		return results;
	}

	public static List<Cell> getByrowkey(String tableName, String rowKey,
			TableInfo tableInfo) {
		List<Cell> cellList = null;
		try {
			init();
			Table table = conn.getTable(TableName.valueOf(tableName));
			Get get = new Get(Bytes.toBytes(rowKey));
			cellList = new ArrayList<Cell>(); 
			
			get.addColumn(Bytes.toBytes(tableInfo.getFamily()),Bytes.toBytes(tableInfo.getQualifier()));
			Result result = table.get(get);
			List<Cell> cells = result.getColumnCells(Bytes.toBytes(tableInfo.getFamily()), Bytes.toBytes(tableInfo.getQualifier()));
			for (Cell cell : cells) {
				cellList.add(cell);
				System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
			}
			
			byte[] bytes =  result.getValue(Bytes.toBytes("base-info"), Bytes.toBytes("name"));
			System.out.println(new String(bytes));
			table.close();
			end();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return cellList;
	}
	public static List<Result> singleColumnValueFilter(String tableName,TableInfo tableInfo)
	{
		init();
		List<Result> rsList=null;
		try {
			Table table = conn.getTable(TableName.valueOf(tableName));
			Scan scan =new Scan();
			rsList = new ArrayList<Result>();
			
			Filter filter = new SingleColumnValueFilter(Bytes.toBytes(tableInfo.getFamily()),
					Bytes.toBytes(tableInfo.getQualifier()), CompareOp.EQUAL, 
					Bytes.toBytes(tableInfo.getValue()));
			scan.setFilter(filter);
		 	ResultScanner resultScanner = table.getScanner(scan);
			for(Result rs : resultScanner)
			{
				rsList.add(rs);
			}
			table.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		end();
		return rsList;
		
	}
	public static void main(String args[])
	{
		TableInfo tableInfo = new TableInfo("base-info","name","wanggang");
		/*List<Cell> cellList = h.getByrowkey("user", "row-key-1", tableInfo);
		for(Cell cell:cellList)
		{
			System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
		}*/
		
		
		/*List<Result> ls = HbaseUtil.singleColumnValueFilter("customer", tableInfo);
		for(Result rs : ls)
		{
			//System.out.println(new String(rs.getValue(Bytes.toBytes("base-info"),Bytes.toBytes("name"))));
			for(Cell cell : rs.rawCells())
			{
				System.out.println("cell:"+cell+",value:"+Bytes.toString(cell.getValueArray(),
						cell.getValueOffset(),
						cell.getValueLength()));
				System.out.println(new String(cell.getRowArray(),cell.getRowOffset(),cell.getRowLength()));
			}
			HbaseUtil.resultToJsonString(rs);
		}*/
		
		
		HbaseUtil.getAll("genre");
	}
	public static String resultToJsonString(Result result)
	{
		String jsonString="";
		
		JsonObject jsonObject = new JsonObject();
		for(Cell cell : result.rawCells())
		{
			/*System.out.println("cell:"+cell+",value:"+Bytes.toString(cell.getValueArray(),
					cell.getValueOffset(),
					cell.getValueLength()));*/
			//System.out.println(new String(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength()));
			/*if(new String(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength()).equals("id"))
			{
				jsonObject.addProperty(new String(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength()),
						Integer.parseInt(new String(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength())));
			}*/
			//else
			//{
				jsonObject.addProperty(new String(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength()),
						new String(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength()));
			//}
			
		}
		System.out.println(jsonObject.toString());	
		return jsonObject.toString();
	}

	public static void end() {
		try {
			conn.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}

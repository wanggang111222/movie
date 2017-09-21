package oracle.demo.oow.hb.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import oracle.demo.oow.bd.pojo.SearchCriteria;
import oracle.demo.oow.bd.to.CustomerGenreMovieTO;
import oracle.demo.oow.bd.to.GenreTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.HbaseDB;
import oracle.demo.oow.bd.util.HbaseUtil;

public class GenreDao {
	private static final int TOP = 50;
	public void insert(GenreTO genreTO)
	{
		HbaseDB hbaseDB = HbaseDB.getInstance();
		Table table = hbaseDB.getTable(ConstantsHBase.TABLE_GENRE);
		Put put = new Put(Bytes.toBytes(genreTO.getId()));
		put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_GENRE), Bytes.toBytes(ConstantsHBase.QUALIFIER_GENRE_NAME), Bytes.toBytes(genreTO.getName()));
		try {
			table.put(put);
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public void insertCustGenreMovie(CustomerGenreMovieTO customerGenreMovieTO){
		HbaseDB hbaseDB = HbaseDB.getInstance();
		Table table = hbaseDB.getTable(ConstantsHBase.TABLE_GENRE);
		Put put = new Put(Bytes.toBytes(customerGenreMovieTO.getGenreId()+"_"+customerGenreMovieTO.getMovieId()));
		put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_GENRE_MOVIE_ID), Bytes.toBytes(customerGenreMovieTO.getMovieId()));
		try {
			table.put(put);
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	public List<String> getMovieIdByGenreID(int genreId)
	{
		HbaseDB hbaseDB = HbaseDB.getInstance();
		Table table = hbaseDB.getTable(ConstantsHBase.TABLE_GENRE);
		Filter filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("^"+genreId+"_"));
		Scan scan= new Scan();
		scan.setFilter(filter);
		List<String> ls = new ArrayList<String>();
		try {
			ResultScanner resultScanner= table.getScanner(scan);
			for(Result r:resultScanner)
			{
				ls.add(String.valueOf(Bytes.toInt(r.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_GENRE_MOVIE_ID)))));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return ls;
		
	}
	/*public List<MovieTO> getMoviesByGenre(int genreId, SearchCriteria sc){
		return getMoviesByGenre(genreId, TOP, sc);
	}
	public List<MovieTO> getMoviesByGenre(int genreId, int movieCount, SearchCriteria sc){
		return
	}*/
	public List<GenreTO> getGenres()
	{
		
    	String genreTOValue = null;  
    	HbaseDB hbaseDB=HbaseDB.getInstance();
    	Table table = hbaseDB.getTable(ConstantsHBase.TABLE_GENRE);
    	List<GenreTO> genreList = new ArrayList<GenreTO>();
    	Filter filter = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_GENRE)));
    	Scan scan = new Scan();
    	scan.setFilter(filter);
    	try {
			ResultScanner results = table.getScanner(scan);
			for(Result r:results)
			{
				GenreTO genreTO = new GenreTO();;
				Result result = r;
				genreTO.setCid("GN");
				genreTO.setName(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_GENRE), Bytes.toBytes(ConstantsHBase.QUALIFIER_GENRE_NAME))));
			    genreTO.setId(Bytes.toInt(result.getRow()));
			    genreList.add(genreTO);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
    	return genreList;
	}
	public static void main(String[] args) {
		GenreDao genreDao = new GenreDao();
		genreDao.getGenres();
	}

}

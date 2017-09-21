package oracle.demo.oow.hb.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import oracle.demo.oow.bd.dao.MovieDAO;
import oracle.demo.oow.bd.to.CastCrewTO;
import oracle.demo.oow.bd.to.CastMovieTO;
import oracle.demo.oow.bd.to.CastTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.HbaseDB;
import oracle.demo.oow.bd.util.KeyUtil;
import oracle.demo.oow.bd.util.StringUtil;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;

public class CastDao {
	/**
	 * @param castTO
	 */
	public void insert(CastTO castTO)
	{
		HbaseDB hbaseDB = HbaseDB.getInstance();
		Table table =hbaseDB.getTable(ConstantsHBase.TABLE_CAST);
		Put put = new Put(Bytes.toBytes(castTO.getId()));
		put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_CAST), Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_NAME), Bytes.toBytes(castTO.getName()));
		List<CastMovieTO> ls = castTO.getCastMovieList();
		List<Put> puts = new ArrayList<Put>();
		Iterator<CastMovieTO> iterator = ls.iterator();
		while(iterator.hasNext())
		{
			CastMovieTO castMovieTO = iterator.next();
			Put put1 = new Put(Bytes.toBytes(castTO.getId()+"_"+castMovieTO.getId()));
			put1.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_MOVIE_ID), Bytes.toBytes(castMovieTO.getId()));
			put1.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_CHARACTER), Bytes.toBytes(castMovieTO.getCharacter()));
			put1.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_ORDER), Bytes.toBytes(castMovieTO.getOrder()));
            puts.add(put1);		
		}
		puts.add(put);
		try {
			table.put(puts);
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	
	public List<CastTO> getMovieCasts(int movieId){
		List<CastTO> castList =new ArrayList<CastTO>();

        String jsonTxt = null;
        if (movieId > -1)
        {
        	HbaseDB hbaseDB = HbaseDB.getInstance();
        	Table table = hbaseDB.getTable(ConstantsHBase.TABLE_CAST);
        	Filter filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("_"+movieId+"$"));
        	Scan scan = new Scan();
        	scan.setFilter(filter);
        	try {
				ResultScanner resultScanner = table.getScanner(scan);
				for(Result result:resultScanner)
				{
					//System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_ORDER))));
					String id = Bytes.toString(result.getRow()).split("_")[0];
					castList.add(getCastById(Integer.parseInt(id)));
				}
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
        return castList;
	}
	
	public CastTO getCastById(int id)
	{
		HbaseDB hbaseDB = HbaseDB.getInstance();
    	Table table = hbaseDB.getTable(ConstantsHBase.TABLE_CAST);
    	Get get = new Get(Bytes.toBytes(id));
    	CastTO castTO = new CastTO();
    	try {
			Result result = table.get(get);
			castTO.setName(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_CAST), Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_NAME))));
			castTO.setId(Bytes.toInt(result.getRow()));
			List<CastMovieTO> ls = getMovieListByCast(id);
			castTO.setCastMovieList(ls);
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
    	System.out.println(castTO.toJsonString());
    	return castTO;
	}
	public List<CastMovieTO> getMovieListByCast(int castId)
	{
		HbaseDB hbaseDB = HbaseDB.getInstance();
    	Table table = hbaseDB.getTable(ConstantsHBase.TABLE_CAST);
    	
    	List<CastMovieTO> ls = new ArrayList<CastMovieTO>();
        Filter filter = new RowFilter(CompareOp.EQUAL,new RegexStringComparator("^"+castId+"_"));
        Scan scan =new Scan();
        scan.setFilter(filter);
        try {
			ResultScanner resultScanner = table.getScanner(scan);
			for(Result r:resultScanner)
			{
				CastMovieTO castMovieTO = new CastMovieTO();
				castMovieTO.setId(Bytes.toInt(r.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_MOVIE_ID))));
				castMovieTO.setCharacter(Bytes.toString(r.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_CHARACTER))));
				castMovieTO.setOrder(Bytes.toInt(r.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_ORDER))));
				ls.add(castMovieTO);
			}
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
        return ls;
        
	}
	public List<MovieTO> getMoviesByCast(int castId){
        List<MovieTO> movieList = new ArrayList<MovieTO>();
        int movieId = 0;
        MovieDao movieDAO = new MovieDao();
        
        List<CastMovieTO> castMovieTOs = getMovieListByCast(castId);
        for (CastMovieTO castMovieTO : castMovieTOs) {
        	MovieTO movieTO = new MovieTO();
            movieId = castMovieTO.getId();
            movieTO = movieDAO.getMovieById(movieId);

            if (movieTO != null) {
                //add to movieList
                movieList.add(movieTO);
            } //if(movieTO!=null)
        } //for
        
		return movieList;
	}
	
	public static void main(String[] args) {
		CastDao castDao = new CastDao();
		System.out.println("Printing all the casts in movieId=857");

        /*List<CastTO> castList = castDao.getMovieCasts(857);
        for (CastTO cTO : castList) {
            System.out.println("\t" + cTO.getName() + " " + cTO.getJsonTxt());
        }*/
		List<MovieTO> movieTOs = castDao.getMoviesByCast(9);
		for (MovieTO cTO : movieTOs) {
            System.out.println("\t" + cTO.getTitle() + " " + cTO.getMovieJsonTxt());
        }
		/*List<CastMovieTO> ls = castDao.getMovieListByCast(9);
		for (CastMovieTO cTO : ls) {
            System.out.println("\t" + cTO.getId() + " " + cTO.toJsonString());
        }*/
	}

}

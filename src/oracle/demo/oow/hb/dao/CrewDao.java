package oracle.demo.oow.hb.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

import oracle.demo.oow.bd.to.CastMovieTO;
import oracle.demo.oow.bd.to.CastTO;
import oracle.demo.oow.bd.to.CrewTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.HbaseDB;

public class CrewDao {
	public void insert(CrewTO crewTO)
	{
		HbaseDB hbaseDB = HbaseDB.getInstance();
		Table table = hbaseDB.getTable(ConstantsHBase.TABLE_CREW);
		List<Put> puts = new ArrayList<Put>();
		Put put = new Put(Bytes.toBytes(crewTO.getId()));
		put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_CREW), Bytes.toBytes(ConstantsHBase.QUALIFIER_CREW_NAME), Bytes.toBytes(crewTO.getName()));
		put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_CREW), Bytes.toBytes(ConstantsHBase.QUALIFIER_CREW_JOB), Bytes.toBytes(crewTO.getJob()));
		List<String> ls = crewTO.getMovieList();
		
		try {
			for(String s:ls)
			{
				Put put1 = new Put(Bytes.toBytes(crewTO.getId()+"_"+s));
				put1.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_CREW_MOVIE_ID), Bytes.toBytes(Integer.parseInt(s)));
				puts.add(put1);
			}
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		puts.add(put);
		
		try {
			table.put(puts);
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public List<CrewTO> getMovieCrews(int movieId)
	{
		List<CrewTO> crewList =new ArrayList<CrewTO>();

        String jsonTxt = null;
        if (movieId > -1)
        {
        	HbaseDB hbaseDB = HbaseDB.getInstance();
        	Table table = hbaseDB.getTable(ConstantsHBase.TABLE_CREW);
        	Filter filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("_"+movieId+"$"));
        	Scan scan = new Scan();
        	scan.setFilter(filter);
        	try {
				ResultScanner resultScanner = table.getScanner(scan);
				for(Result result:resultScanner)
				{
					String id = Bytes.toString(result.getRow()).split("_")[0];
					crewList.add(getCrewById(Integer.parseInt(id)));
				}
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
        return crewList;
	}
	public CrewTO getCrewById(int id)
	{
		HbaseDB hbaseDB = HbaseDB.getInstance();
    	Table table = hbaseDB.getTable(ConstantsHBase.TABLE_CREW);
    	Get get = new Get(Bytes.toBytes(id));
    	CrewTO crewTO = new CrewTO();
    	try {
			Result result = table.get(get);
			crewTO.setName(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_CREW), Bytes.toBytes(ConstantsHBase.QUALIFIER_CREW_NAME))));
			crewTO.setId(Bytes.toInt(result.getRow()));
			crewTO.setJob(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_CREW), Bytes.toBytes(ConstantsHBase.QUALIFIER_CREW_JOB))));
			List<String> ls = getMovieListByCrew(id);
			crewTO.setMovieList(ls);
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
    	return crewTO;
	}
	public List<String> getMovieListByCrew(int crewId)
	{
		HbaseDB hbaseDB = HbaseDB.getInstance();
    	Table table = hbaseDB.getTable(ConstantsHBase.TABLE_CREW);
    	List<String> ls = new ArrayList<String>();
    	Filter filter = new RowFilter(CompareOp.EQUAL,new RegexStringComparator("^"+crewId+"_"));
    	Scan scan = new Scan();
    	scan.setFilter(filter);
    	try {
			ResultScanner resultScanner = table.getScanner(scan);
			for(Result r : resultScanner)
			{
				String id = String.valueOf(Bytes.toInt(r.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_MOVIE),Bytes.toBytes(ConstantsHBase.QUALIFIER_CREW_MOVIE_ID))));
				ls.add(id);
			}
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
    	return ls;
	}
	public List<MovieTO> getMoviesByCrew(int crewId){
		List<MovieTO> movieList = new ArrayList<MovieTO>();
        MovieTO movieTO = null;
        MovieDao movieDAO = new MovieDao();
        
        List<String> castMovieTOs = getMovieListByCrew(crewId);
        for (String string : castMovieTOs) {
        	movieTO = movieDAO.getMovieById(string);
            if (movieTO != null) {
                //add to movieList
                movieList.add(movieTO);
            } //if(movieTO!=null)
        } //for
		return movieList;
	}
	public static void main(String[] args) {
		CrewDao crewDao = new CrewDao();
		System.out.println("Printing all the casts in movieId=857");
        List<CrewTO> crewList = crewDao.getMovieCrews(857);
        for (CrewTO cwTO : crewList) {
            System.out.println("\t" + cwTO.getName() + " " + cwTO.getJsonTxt());
        }
	}

}

package oracle.demo.oow.hb.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import oracle.demo.oow.bd.to.CastCrewTO;
import oracle.demo.oow.bd.to.CastTO;
import oracle.demo.oow.bd.to.CrewTO;
import oracle.demo.oow.bd.to.GenreTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.HbaseDB;
import oracle.demo.oow.bd.util.KeyUtil;
import oracle.demo.oow.bd.util.StringUtil;
import oracle.demo.oow.bd.util.parser.URLReader;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;

public class MovieDao {
	
	public void insert(MovieTO movieTO)
	{
		HbaseDB hbaseDB = HbaseDB.getInstance();
		Table table = hbaseDB.getTable(ConstantsHBase.TABLE_MOVIE);
		List<Put> puts = new ArrayList<Put>();
		
		Put put = new Put(Bytes.toBytes(movieTO.getId()));
		put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_ORIGINAL_TITLE), Bytes.toBytes(movieTO.getTitle()));
		put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_OVERVIEW), Bytes.toBytes(movieTO.getOverview()));
		put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_POSTER_PATH), Bytes.toBytes(movieTO.getPosterPath()));
		put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_RELEASE_DATE), Bytes.toBytes(movieTO.getReleasedYear()));
		put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_VOTE_COUNT), Bytes.toBytes(movieTO.getVoteCount()));
		put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_RUNTIME), Bytes.toBytes(movieTO.getRunTime()));
		put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_POPULARITY), Bytes.toBytes(movieTO.getPopularity()));
		for(GenreTO genreTO:movieTO.getGenres())
		{
			Put put1 = new Put(Bytes.toBytes(movieTO.getId()+"_"+genreTO.getId()));
			put1.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_GENRE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_GENRE_ID), Bytes.toBytes(genreTO.getId()));
			put1.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_GENRE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_GENRE_NAME), Bytes.toBytes(genreTO.getName()));
			puts.add(put1);
		}
		puts.add(put);
		try {
			table.put(puts);
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	/*	Put put2 = new Put(Bytes.toBytes(ConstantsHBase.ROW_KEY_MOVIE_MOVIEID_CREWID));
		put2.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_CREW), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_CREW_ID), Bytes.toBytes(movieTO.getTitle()));
		Put put3 = new Put(Bytes.toBytes(ConstantsHBase.ROW_KEY_MOVIE_MOVIEID_CASTID));
		put3.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_CAST), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_CAST_ID), Bytes.toBytes(movieTO.getTitle()));*/
	}
	
	public MovieTO getMovieById(String movieIdStr) 
	{
		int movieId = 0;
        if (StringUtil.isNotEmpty(movieIdStr)) {
            try {
                movieId = Integer.parseInt(movieIdStr);
            } catch (NumberFormatException ne) {
                movieId = 0;
            } //EOF try/catch


        } //EOF if
        return getMovieById(movieId);
	}

	public MovieTO getMovieById(int movieId)
	{
		List<CastTO> castList = null;
        List<CrewTO> crewList = null;
        MovieTO movieTO = new MovieTO();
        CastDao castDAO = new CastDao();
        CrewDao crewDAO = new CrewDao();
        CastCrewTO castCrewTO = new CastCrewTO();
        HbaseDB hbaseDB =HbaseDB.getInstance();
        Table table = hbaseDB.getTable(ConstantsHBase.TABLE_MOVIE);
        Filter filter = new RowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(movieId)));
        Scan scan = new Scan();
        scan.setFilter(filter);
        try {
			ResultScanner resultScanner = table.getScanner(scan);
			for(Result result:resultScanner)
			{
				movieTO.setId(Bytes.toInt(result.getRow()));
				movieTO.setTitle(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_ORIGINAL_TITLE))));
				movieTO.setOverview(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_OVERVIEW))));
				movieTO.setPosterPath(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_POSTER_PATH))));
				movieTO.setDate(String.valueOf(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_RELEASE_DATE)))));
				movieTO.setVoteCount(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_VOTE_COUNT))));
				movieTO.setRunTime(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_RUNTIME))));
				movieTO.setPopularity(Bytes.toDouble(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_POPULARITY))));
				ArrayList<GenreTO> ls = new ArrayList<GenreTO>();
				Filter filter2 = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("^"+String.valueOf(Bytes.toInt(result.getRow()))+"_*"));
				Scan scan2 = new Scan();
				scan2.setFilter(filter2);
				ResultScanner resultScanner2 = table.getScanner(scan2);
				for(Result result2 : resultScanner2)
				{
					GenreTO genreTO = new GenreTO();
					genreTO.setId(Bytes.toInt(result2.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_GENRE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_GENRE_ID))));
					genreTO.setName(Bytes.toString(result2.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_GENRE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_GENRE_NAME))));
					ls.add(genreTO);
				}
				movieTO.setGenres(ls);
				
				
			}
			if (movieTO != null && !URLReader.isInternetReachable())
                movieTO.setPosterPath("");
			castList = castDAO.getMovieCasts(movieId);
            castCrewTO.setCastList(castList);
            
            crewList = crewDAO.getMovieCrews(movieId);
            castCrewTO.setCrewList(crewList);
            
            movieTO.setCastCrewTO(castCrewTO);
            table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return movieTO;
	}
	
	public static void main(String[] args) {
		MovieDao movieDao = new MovieDao();
		MovieTO movieTO = movieDao.getMovieById(9997);
		System.out.println(movieTO.getMovieJsonTxt());
	}
}

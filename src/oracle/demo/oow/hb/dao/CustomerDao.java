package oracle.demo.oow.hb.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import oracle.demo.oow.bd.dao.MovieDAO;
import oracle.demo.oow.bd.pojo.ActivityType;
import oracle.demo.oow.bd.pojo.BooleanType;
import oracle.demo.oow.bd.pojo.RatingType;
import oracle.demo.oow.bd.to.ActivityTO;
import oracle.demo.oow.bd.to.CustomerGenreMovieTO;
import oracle.demo.oow.bd.to.CustomerGenreTO;
import oracle.demo.oow.bd.to.CustomerTO;
import oracle.demo.oow.bd.to.GenreMovieTO;
import oracle.demo.oow.bd.to.GenreTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.to.ScoredGenreTO;
import oracle.demo.oow.bd.util.HbaseDB;
import oracle.demo.oow.bd.util.KeyUtil;
import oracle.demo.oow.bd.util.StringUtil;
import oracle.kv.impl.util.RateLimitingLogger;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.TableIterator;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.sun.org.apache.regexp.internal.recompile;

public class CustomerDao {
	
    private final static String TABLENAME="customer";
    private static int MOVIE_MAX_COUNT = 25;
    
	public void insert(CustomerTO customerTO)
	{
		HbaseDB hbase = HbaseDB.getInstance();
		Table table = hbase.getTable(TABLENAME);
		Put put = new Put(Bytes.toBytes(customerTO.getId()));
		Put put1 = new Put(Bytes.toBytes(customerTO.getUserName()));
		
		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(customerTO.getName()));
		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("email"), Bytes.toBytes(customerTO.getEmail()));
		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("username"), Bytes.toBytes(customerTO.getUserName()));
		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("password"), Bytes.toBytes(customerTO.getPassword()));
		
		put1.addColumn(Bytes.toBytes("id"), Bytes.toBytes("id"), Bytes.toBytes(customerTO.getId()));
		
		List<Put> puts =new ArrayList<Put>();
		puts.add(put);
		puts.add(put1);
		
		try {
			table.put(puts);
			table.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	public CustomerTO getCustomerByCredential(String username, String password){
		HbaseDB hbase = HbaseDB.getInstance();
		Table table = hbase.getTable(TABLENAME);
		int id = getIdByUsername(username);
		CustomerTO customerTO = getInfoById(id);
		if(!customerTO.getPassword().equals(password))
		{
			customerTO=null;
		}
		try {
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return customerTO;
	}
	public CustomerTO getInfoById(int id)
	{
		HbaseDB hbase = HbaseDB.getInstance();
		Table table =hbase.getTable(TABLENAME);
		Get get = new Get(Bytes.toBytes(id));
		CustomerTO customerTO = new CustomerTO();
		
		try {
			Result result = table.get(get);
			customerTO.setId(id);
			customerTO.setEmail(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("email"))));
			customerTO.setName(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))));
			customerTO.setPassword(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("password"))));
			customerTO.setUserName(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("username"))));
			System.out.println(customerTO.getPassword());
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return customerTO;
	}
	
	public int getIdByUsername(String username) {
		HbaseDB hbase = HbaseDB.getInstance();
        Table table = hbase.getTable(TABLENAME);
        int i=0;
        try {
			Get get = new Get(Bytes.toBytes(username));
			Result results = table.get(get);
			i=Bytes.toInt(results.getValue(Bytes.toBytes("id"), Bytes.toBytes("id")));
			table.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return i;
        
	}
	public ActivityTO getMovieRating(int custId, int movieId) {
		HbaseDB hbaseDB =HbaseDB.getInstance();
		Table table = hbaseDB.getTable(ConstantsHBase.TABLE_ACTIVITY);
		List<Filter> filters = new ArrayList<Filter>();
		
		Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID), CompareOp.EQUAL, Bytes.toBytes(movieId));
		Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID),CompareOp.EQUAL,Bytes.toBytes(custId));
		
		filters.add(filter1);
		filters.add(filter2);
		FilterList filterList = new FilterList(filters);
		Scan scan = new Scan();
		scan.setFilter(filterList);
		ActivityTO activityTO = new ActivityTO();
		ActivityType activityType;
		RatingType ratingType;
		BooleanType booleanType;
		try {
			ResultScanner scanner = table.getScanner(scan);
			Iterator<Result> iterator = scanner.iterator();
			if(iterator.hasNext())
			{
				Result r = iterator.next();
				activityType=ActivityType.getType(Bytes.toInt((r.getValue(Bytes.toBytes((ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY)), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY)))));
				ratingType =RatingType.getType(Bytes.toInt(r.getValue(Bytes.toBytes((ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY)), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_RATING))));
				booleanType =BooleanType.getType(Bytes.toString(r.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_RECOMMENDED))));
				activityTO.setActivity(activityType);
				activityTO.setCustId(Bytes.toInt(r.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID))));
				activityTO.setGenreId(Bytes.toInt(r.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_GENRE_ID))));
				activityTO.setMovieId(Bytes.toInt(r.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID))));
				activityTO.setPosition(Bytes.toInt(r.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_POSITION))));
				activityTO.setPrice(Bytes.toInt(r.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_PRICE))));
				activityTO.setRating(ratingType);
				activityTO.setRecommended(booleanType);
				activityTO.setTableId(Bytes.toString(r.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_TABLE_ID))));
				activityTO.setTimeStamp(Bytes.toLong(r.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_TIME))));	
			}
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return activityTO;
		
    } //getMovieRating
	public List<GenreMovieTO> getMovies4Customer(int custId, int movieMaxCount, int genreMaxCount){
		
		List<GenreMovieTO> genreMovieList = new ArrayList<GenreMovieTO>();
		int genreId = 0;
        String name = null;
        GenreMovieTO genreMovieTO = null;
        GenreTO genreTO = null;
        List<MovieTO> movieList = null;
        int count = 0;
        
        CustomerGenreTO customerGenreTO3 = new CustomerGenreTO();
        customerGenreTO3 = getGenreByCustID(custId);
        String jsonTxt=customerGenreTO3.toJsonString();
        if(StringUtil.isEmpty(jsonTxt))
        {
        	CustomerGenreTO customerGenreTO2 = new CustomerGenreTO();
        	customerGenreTO2 = getGenreByCustID(0);
        	jsonTxt = (customerGenreTO2!=null?customerGenreTO2.toJsonString():null);
        }
        if (StringUtil.isNotEmpty(jsonTxt))
        {
        	System.out.println("jsonTxt2"+jsonTxt);
        	CustomerGenreTO customerGenreTO = new CustomerGenreTO(jsonTxt);

            for (ScoredGenreTO scoredGenreTO : customerGenreTO.getScoredGenreList()) {

                count++;
                //create genreMovieTO object
                genreMovieTO = new GenreMovieTO();
                //Get values from ScoredGenreTO and assign it to GenreTO
                genreId = scoredGenreTO.getId();
                name = scoredGenreTO.getName();

                //create GenreTO
                genreTO = new GenreTO();
                genreTO.setId(genreId);
                genreTO.setName(name);

                //get Movie list by genre
                movieList = this.getMovies4CustomerByGenre(custId, genreId, movieMaxCount);

                //set GenreTO & MovieTO list to GenreMovieTO
                genreMovieTO.setGenreTO(genreTO);
                genreMovieTO.setMovieList(movieList);

                //add genreMovieTO to the list
                genreMovieList.add(genreMovieTO);

                //Break the loop if you have got M top genres from the list
                if (count >= genreMaxCount) {
                    break;
                }
            }
        }
         else {
                System.out.println("Error: Default recommendation data is not fed into DB yet:\nPlease run MovieDAO.insertTopMoviesPerGenre() method first to seed the default recommendation.");
         }
        return genreMovieList;
	}
	public CustomerGenreTO getGenreByCustID(int custId)
	{
		CustomerGenreTO customerGenreTO=new CustomerGenreTO();
		customerGenreTO.setId(custId);
		customerGenreTO.setScoredGenreList(getScoreGenreList(custId));
		return customerGenreTO;
		
	}
	public List<ScoredGenreTO> getScoreGenreList(int custId)
	{
		HbaseDB hbaseDB = HbaseDB.getInstance();
		List<ScoredGenreTO> scoredGenreTOs = new ArrayList<ScoredGenreTO>();
		Table table= hbaseDB.getTable(TABLENAME);
		Filter filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("^"+custId+"_"));
		Scan scan = new Scan();
		scan.setFilter(filter);
		try {
			ResultScanner resultScanner = table.getScanner(scan);
			Iterator<Result> iterator = resultScanner.iterator();
			while(iterator.hasNext())
			{
				Result result = iterator.next();
				ScoredGenreTO scoredGenreTO=new ScoredGenreTO();
				scoredGenreTO.setId(Bytes.toInt(result.getValue(Bytes.toBytes("genre"), Bytes.toBytes(ConstantsHBase.QUALIFIER_USER_GENRE_ID))));
				scoredGenreTO.setName(Bytes.toString(result.getValue(Bytes.toBytes("genre"), Bytes.toBytes(ConstantsHBase.QUALIFIER_USER_GENRE_NAME))));
				scoredGenreTO.setScore(Bytes.toInt(result.getValue(Bytes.toBytes("genre"), Bytes.toBytes(ConstantsHBase.QUALIFIER_USER_SCORE))));
				scoredGenreTOs.add(scoredGenreTO);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return scoredGenreTOs;
	}
	public List<MovieTO> getMovies4CustomerByGenre(int custId, int genreId){
		return getMovies4CustomerByGenre(custId, genreId, MOVIE_MAX_COUNT);
	}
	public List<MovieTO> getMovies4CustomerByGenre(int custId, int genreId, int maxCount){
		HbaseDB hbaseDB = HbaseDB.getInstance();
		GenreDao genreDao= new GenreDao();
		MovieDao movieDao = new MovieDao();
		ActivityTO activityTO=null;
		List<MovieTO> movieTOs = new ArrayList<MovieTO>();
		Table table = hbaseDB.getTable(TABLENAME);
		int count = 0;
		Filter filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("^"+custId+"_"));
		Scan scan = new Scan();
		scan.setFilter(filter);
		try {
			ResultScanner resultScanner = table.getScanner(scan);
			for(Result r:resultScanner)
			{
				if(genreId==Bytes.toInt(r.getValue(Bytes.toBytes("genre"), Bytes.toBytes(ConstantsHBase.QUALIFIER_USER_GENRE_ID))))
				{
					List<String> ls = genreDao.getMovieIdByGenreID(genreId);
					if(ls.size()==0)
					{
						break;
					}
					for(String s:ls)
					{
						MovieTO movieTO = new MovieTO();
						movieTO=movieDao.getMovieById(Integer.parseInt(s));
						if (movieTO != null) {

			                if (StringUtil.isNotEmpty(movieTO.getPosterPath())) {
			                    movieTO.setOrder(100);
			                } else {
			                    movieTO.setOrder(0);
			                }

			                //Check to see if user has already rated this movie
			                activityTO = this.getMovieRating(custId, Integer.parseInt(s));
			                if (activityTO != null) {
			                    movieTO.setUserRating(activityTO.getRating());
			                }

			                //add movieTO to the list
			                movieTOs.add(movieTO);

			                //check if count is less than or equals to maxCount or not
			                if (++count >= maxCount) {
			                    break;
			                }
			            } //if(movieTO!=null)
						movieTOs.add(movieDao.getMovieById(Integer.parseInt(s)));
					}
 				}
			}
		    table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		 Collections.sort(movieTOs);
		return movieTOs;
		
	}
	public void insertMovieRating(int custId, int movieId, ActivityTO activityTO){
		if (activityTO != null) {
            ActivityDao activityDao = new ActivityDao();
            activityDao.insertCustomerActivity(activityTO);
        }
	}
	public void insertMovies4CustomerByGenre(CustomerGenreTO customerGenreTO){
		 HbaseDB hbaseDB =HbaseDB.getInstance();
		 Table table =hbaseDB.getTable(TABLENAME);
		 List<ScoredGenreTO> ls = customerGenreTO.getScoredGenreList();
		 for(ScoredGenreTO scoredGenreTO:ls)
		 {
			 Put put = new Put(Bytes.toBytes(customerGenreTO.getId()+"_"+scoredGenreTO.getId()));
			 put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_USER_GENRE), Bytes.toBytes(ConstantsHBase.QUALIFIER_USER_GENRE_ID), Bytes.toBytes(scoredGenreTO.getId()));
			 put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_USER_GENRE), Bytes.toBytes(ConstantsHBase.QUALIFIER_USER_GENRE_NAME), Bytes.toBytes(scoredGenreTO.getName()));
			 put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_USER_GENRE), Bytes.toBytes(ConstantsHBase.QUALIFIER_USER_SCORE), Bytes.toBytes(scoredGenreTO.getScore()));
			 try {
				table.put(put);
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		 }
		 
	}
	public static void main(String[] args) {
		CustomerDao customerDao = new CustomerDao();
		/*CustomerTO customerTO=customerDao.getCustomerByCredential("guest1", "welcome1");
		 String jsonTxt = customerTO.getJsonTxt();
        System.out.print(jsonTxt);*/
		List<MovieTO> movieTOs =customerDao.getMovies4CustomerByGenre(1321610, 18, 10);
		for(MovieTO movieTO:movieTOs)
		{
			System.out.println(movieTO);
		}
	}
}

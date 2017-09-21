package oracle.demo.oow.hb.dao;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import oracle.demo.oow.bd.constant.KeyConstant;
import oracle.demo.oow.bd.pojo.ActivityType;
import oracle.demo.oow.bd.pojo.BooleanType;
import oracle.demo.oow.bd.pojo.RatingType;
import oracle.demo.oow.bd.to.ActivityTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.FileWriterUtil;
import oracle.demo.oow.bd.util.HbaseDB;
import oracle.demo.oow.bd.util.KeyUtil;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;

public class ActivityDao {

	public void insertCustomerActivity(ActivityTO activityTO) {
		int custId = 0;
		int movieId = 0;
		String jsonTxt = null;
		ActivityType activityType = null;

		CustomerDao customerDao = new CustomerDao();

		if (activityTO != null) {
			jsonTxt = activityTO.getJsonTxt();

			/**
			 * This system out should write the content to the application log
			 * file.
			 */
			FileWriterUtil.writeOnFile(activityTO.getActivityJsonOriginal()
					.toString());

			custId = activityTO.getCustId();
			movieId = activityTO.getMovieId();

			if (custId > 0 && movieId > 0) {
				activityType = activityTO.getActivity();

				switch (activityType) {
				case STARTED_MOVIE:
					activityTO.setTableId(KeyConstant.CUSTOMER_CURRENT_WATCH_LIST);
					deleteCustomerBrowseList(custId, movieId);
					break;
				case PAUSED_MOVIE:
                    //update the current position of the movie into current watch list
                    activityTO.setTableId(KeyConstant.CUSTOMER_CURRENT_WATCH_LIST);
                    break;
				case COMPLETED_MOVIE:
					activityTO.setTableId( KeyConstant.CUSTOMER_HISTORICAL_WATCH_LIST);
					deleteCustomerBrowseList(custId, movieId);
					break;
				case RATE_MOVIE:
                    //insert user rating for the movie in the CT_MV table
                    
                    break;
				case BROWSED_MOVIE:
					// insert browse information
					activityTO.setTableId(KeyConstant.CUSTOMER_BROWSE_LIST);
					break;

				}
				HbaseDB hbaseDB = HbaseDB.getInstance();
				long id = hbaseDB.getId(ConstantsHBase.TABLE_GID,
						ConstantsHBase.FAMILY_GID_GID,
						ConstantsHBase.QUALIFIER_GID_ACTIVITY_ID);
				Table table = hbaseDB
						.getTable(ConstantsHBase.TABLE_ACTIVITY);

				Put put = new Put(Bytes.toBytes(id));
				put.addColumn(
						Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID),
						Bytes.toBytes(activityTO.getMovieId()));
				put.addColumn(
						Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID),
						Bytes.toBytes(activityTO.getCustId()));
				put.addColumn(
						Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY),
						Bytes.toBytes(activityTO.getActivity().getValue()));
				put.addColumn(
						Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_GENRE_ID),
						Bytes.toBytes(activityTO.getGenreId()));
				put.addColumn(
						Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_POSITION),
						Bytes.toBytes(activityTO.getPosition()));
				put.addColumn(
						Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_PRICE),
						Bytes.toBytes(activityTO.getPrice()));
				put.addColumn(
						Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_RATING),
						Bytes.toBytes(activityTO.getRating().getValue()));
				put.addColumn(
						Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_RECOMMENDED),
						Bytes.toBytes(activityTO.isRecommended().getValue()));
				put.addColumn(
						Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_TIME),
						Bytes.toBytes(activityTO.getTimeStamp()));
				put.addColumn(
						Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_TABLE_ID),
						Bytes.toBytes(activityTO.getTableId()));

				List<Put> puts = new ArrayList<Put>();
				puts.add(put);

				try {
					table.put(puts);
					table.close();
				} catch (IOException e) {
					e.printStackTrace();
				}

			} // if (custId > 0 && movieId > 0)

		}// if (activityTO != null)
	} // insetCustomerActivity

	public void deleteCustomerBrowseList(int custId,int movieId)
	{
		HbaseDB hbaseDB = HbaseDB.getInstance();
		List<Filter> filters = new ArrayList<Filter>();
		
		Table table = hbaseDB.getTable(ConstantsHBase.TABLE_ACTIVITY);
		Filter filter1 = new SingleColumnValueFilter(
				Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
				Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID),
				CompareOp.EQUAL, Bytes.toBytes(custId));
		Filter filter2 = new SingleColumnValueFilter(
				Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
				Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID),
				CompareOp.EQUAL, Bytes.toBytes(movieId));
		Filter filter3 = new SingleColumnValueFilter(
				Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
				Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_TABLE_ID),
				CompareOp.EQUAL, Bytes.toBytes("CT_BL"));
		filters.add(filter1);
		filters.add(filter2);
		filters.add(filter3);
		FilterList filterList = new FilterList(filters);
		Scan scan = new Scan();
		scan.setFilter(filterList);
		
		try {
			ResultScanner resultScanner = table.getScanner(scan);
			for (Result r : resultScanner) {
				int id = Bytes.toInt(r.getRow());
				Delete delete = new Delete(Bytes.toBytes(id));
				table.delete(delete);
			}
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	public List<MovieTO> getCustomerBrowseList(int custId) {
		HbaseDB hbaseDB = HbaseDB.getInstance();
		MovieDao movieDao = new MovieDao();
		List<MovieTO> movieList = new ArrayList<MovieTO>();
		Table table = hbaseDB.getTable(ConstantsHBase.TABLE_ACTIVITY);
		List<Filter> filters = new ArrayList<Filter>();
		Filter filter1 = new SingleColumnValueFilter(
				Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
				Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_TABLE_ID),
				CompareOp.EQUAL, Bytes.toBytes("CT_BL"));
		Filter filter2 = new SingleColumnValueFilter(
				Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
				Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID),
				CompareOp.EQUAL, Bytes.toBytes(custId));
		filters.add(filter1);
		filters.add(filter2);
		FilterList filterList = new FilterList(filters);
		Scan scan = new Scan();
		scan.setFilter(filterList);
		try {
			ResultScanner resultScanner = table.getScanner(scan);
			for (Result r : resultScanner) {
				MovieTO movieTO = new MovieTO();
				int movieId = Bytes
						.toInt(r.getValue(
								Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
								Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID)));
				movieTO = movieDao.getMovieById(movieId);
				movieList.add(movieTO);
			}
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return movieList;
	}

	public List<MovieTO> getCustomerCurrentWatchList(int custId) {
		HbaseDB hbaseDB = HbaseDB.getInstance();
		MovieDao movieDao = new MovieDao();
		List<MovieTO> movieList = new ArrayList<MovieTO>();
		Table table = hbaseDB.getTable(ConstantsHBase.TABLE_ACTIVITY);
		List<Filter> filters = new ArrayList<Filter>();
		Filter filter1 = new SingleColumnValueFilter(
				Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
				Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_TABLE_ID),
				CompareOp.EQUAL, Bytes.toBytes("CT_CWL"));
		Filter filter2 = new SingleColumnValueFilter(
				Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
				Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID),
				CompareOp.EQUAL, Bytes.toBytes(custId));
		filters.add(filter1);
		filters.add(filter2);
		FilterList filterList = new FilterList(filters);
		Scan scan = new Scan();
		scan.setFilter(filterList);
		try {
			ResultScanner resultScanner = table.getScanner(scan);
			for (Result r : resultScanner) {
				MovieTO movieTO = new MovieTO();
				int movieId = Bytes
						.toInt(r.getValue(
								Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
								Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID)));
				movieTO = movieDao.getMovieById(movieId);
				movieList.add(movieTO);
			}
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return movieList;
	}

	public List<MovieTO> getCommonPlayList() {
		long timeStamp = System.currentTimeMillis();
		DateFormat formatter = new SimpleDateFormat("yyyyMMdd");
		String yyyyMMdd = formatter.format(new Date());
		HbaseDB hbaseDB = HbaseDB.getInstance();
		MovieDao movieDao = new MovieDao();
		List<MovieTO> movieList = new ArrayList<MovieTO>();
		Table table = hbaseDB.getTable(ConstantsHBase.TABLE_ACTIVITY);
        List<Filter> filters = new ArrayList<Filter>();
		Filter filter1= new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_TABLE_ID), CompareOp.EQUAL, Bytes.toBytes("CM_CWL"));
		Filter filter2= new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_TIME), CompareOp.EQUAL, Bytes.toBytes(yyyyMMdd));
		filters.add(filter1);
		filters.add(filter2);
		FilterList filterList = new FilterList(filters);
		Scan scan = new Scan();
		scan.setFilter(filterList);
		try {
			ResultScanner resultScanner = table.getScanner(scan);
			for(Result r : resultScanner)
			{
				MovieTO movieTO = new MovieTO();
				int movieId = Bytes
						.toInt(r.getValue(
								Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
								Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID)));
				movieTO = movieDao.getMovieById(movieId);
				movieList.add(movieTO);
			}
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return movieList;
	}
	public List<MovieTO> getCustomerHistoricWatchList(int custId){
		HbaseDB hbaseDB = HbaseDB.getInstance();
		MovieDao movieDao = new MovieDao();
		List<MovieTO> movieList = new ArrayList<MovieTO>();
		Table table = hbaseDB.getTable(ConstantsHBase.TABLE_ACTIVITY);
		List<Filter> filters = new ArrayList<Filter>();
		Filter filter1 = new SingleColumnValueFilter(
				Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
				Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_TABLE_ID),
				CompareOp.EQUAL, Bytes.toBytes("CT_HWL"));
		Filter filter2 = new SingleColumnValueFilter(
				Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
				Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID),
				CompareOp.EQUAL, Bytes.toBytes(custId));
		filters.add(filter1);
		filters.add(filter2);
		FilterList filterList = new FilterList(filters);
		Scan scan = new Scan();
		scan.setFilter(filterList);
		try {
			ResultScanner resultScanner = table.getScanner(scan);
			for (Result r : resultScanner) {
				MovieTO movieTO = new MovieTO();
				int movieId = Bytes
						.toInt(r.getValue(
								Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
								Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID)));
				movieTO = movieDao.getMovieById(movieId);
				movieList.add(movieTO);
			}
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return movieList;
	}
	public ActivityTO getActivityTO(int custId, int movieId){
		ActivityTO activityTO = new ActivityTO();;
        HbaseDB hbaseDB = HbaseDB.getInstance();
        Table table = hbaseDB.getTable(ConstantsHBase.TABLE_ACTIVITY);
        List<Filter> filters = new ArrayList<Filter>();
		Filter filter1 = new SingleColumnValueFilter(
				Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
				Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_TABLE_ID),
				CompareOp.EQUAL, Bytes.toBytes("CT_CWL"));
		Filter filter2 = new SingleColumnValueFilter(
				Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
				Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID),
				CompareOp.EQUAL, Bytes.toBytes(custId));
		Filter filter3 = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
				Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID),
				CompareOp.EQUAL, Bytes.toBytes(movieId));
		filters.add(filter1);
		filters.add(filter2);
		filters.add(filter3);
		FilterList filterList = new FilterList(filters);
		Scan scan = new Scan();
		scan.setFilter(filterList);
		try {
			ResultScanner resultScanner =table.getScanner(scan);
			for(Result r : resultScanner)
			{
				ActivityType activityType = ActivityType.getType(Bytes.toInt(r.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY))));
				activityTO.setActivity(activityType);
				activityTO.setCustId(custId);
				activityTO.setGenreId(Bytes.toInt(r.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_GENRE_ID))));
				activityTO.setMovieId(movieId);
				activityTO.setPosition(Bytes.toInt(r.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_POSITION))));
				activityTO.setPrice(Bytes.toInt(r.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_PRICE))));
				RatingType ratingType=RatingType.getType(Bytes.toInt(r.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_RATING))));
				activityTO.setRating(ratingType);
				BooleanType booleanType =BooleanType.getType(Bytes.toString(r.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_RECOMMENDED))));
				activityTO.setRecommended(booleanType);
				activityTO.setTableId("CT_CWL");
				activityTO.setTimeStamp(Bytes.toLong(r.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_TIME))));
				table.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return activityTO;
	}
	public static void main(String[] args) {
		ActivityDao activityDao = new ActivityDao();
		List<MovieTO> ls = activityDao.getCustomerBrowseList(1255601);
		for (MovieTO m : ls) {
			System.out.println(m.toJsonString());
		}
	}
}

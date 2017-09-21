package oracle.demo.oow.bd.loader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import oracle.demo.oow.bd.constant.Constant;
import oracle.demo.oow.bd.dao.BaseDAO;
import oracle.demo.oow.bd.dao.CustomerDAO;
import oracle.demo.oow.bd.dao.MovieDAO;
import oracle.demo.oow.bd.to.CastTO;
import oracle.demo.oow.bd.to.CrewTO;
import oracle.demo.oow.bd.to.CustomerGenreMovieTO;
import oracle.demo.oow.bd.to.CustomerGenreTO;
import oracle.demo.oow.bd.to.CustomerTO;
import oracle.demo.oow.bd.to.GenreTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.to.ScoredGenreTO;
import oracle.demo.oow.bd.util.HbaseDB;
import oracle.demo.oow.bd.util.KeyUtil;
import oracle.demo.oow.hb.dao.CastDao;
import oracle.demo.oow.hb.dao.ConstantsHBase;
import oracle.demo.oow.hb.dao.CrewDao;
import oracle.demo.oow.hb.dao.CustomerDao;
import oracle.demo.oow.hb.dao.GenreDao;
import oracle.demo.oow.hb.dao.MovieDao;
import oracle.kv.Version;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableIterator;

public class HbaseLoader extends BaseDAO {
	public static void main(String[] args) {
		HbaseLoader hbaseLoader = new HbaseLoader();
		/*hbaseLoader.createCustomerTable();*/
		
		/*try {
			hbaseLoader.loadCustomerData();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		//hbaseLoader.createCustomerTable();
		
		/*try {
			hbaseLoader.loadMovieData();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		/*hbaseLoader.createCustomerTable();*/
		
		/*try {
			hbaseLoader.loadCastData();
		} catch (IOException e) {
			e.printStackTrace();
		}*/
		
		/*hbaseLoader.createCustomerTable();*/
		
		/*try {
			hbaseLoader.loadCrewData();
		} catch (IOException e) {
			e.printStackTrace();
		}*/
		
		/*hbaseLoader.createCustomerTable();*/
		/*hbaseLoader.loadGenreData();*/
		/*hbaseLoader.loadCustomerGenreData();*/
		hbaseLoader.loadCustMovieGenre();
	}

	private void createCustomerTable() {
		HbaseDB hbaseDB = HbaseDB.getInstance();
		/*String columFamilys[] = { "id","info" ,"genre"};
		hbaseDB.createTable("customer", columFamilys);*/
		
		/*String columFamilys[] = { ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY };
		hbaseDB.createTable(ConstantsHBase.TABLE_ACTIVITY, columFamilys);*/
		
		/*String columFamilys[] = { ConstantsHBase.FAMILY_GID_GID };
		hbaseDB.createTable(ConstantsHBase.TABLE_GID, columFamilys);*/
		
		/*String columFamilys[] = { ConstantsHBase.FAMILY_MOVIE_MOVIE,ConstantsHBase.FAMILY_MOVIE_GENRE,ConstantsHBase.FAMILY_MOVIE_CREW,ConstantsHBase.FAMILY_MOVIE_CAST };
		hbaseDB.createTable(ConstantsHBase.TABLE_MOVIE, columFamilys);*/
		
		/*String columFamilys[] = { ConstantsHBase.FAMILY_CAST_CAST,ConstantsHBase.FAMILY_CAST_MOVIE};
		hbaseDB.createTable(ConstantsHBase.TABLE_CAST, columFamilys);*/
		
		/*String columFamilys[] = { ConstantsHBase.FAMILY_CREW_CREW,ConstantsHBase.FAMILY_CREW_MOVIE};
		hbaseDB.createTable(ConstantsHBase.TABLE_CREW, columFamilys);*/
		
		String columFamilys[] = { ConstantsHBase.FAMILY_GENRE_GENRE,ConstantsHBase.FAMILY_GENRE_MOVIE};
		hbaseDB.createTable(ConstantsHBase.TABLE_GENRE, columFamilys);
	}

	private void loadCustomerData() throws IOException {
		FileReader fr = null;
		Version version = null;
		CustomerDao customerDao = new CustomerDao();

		try {

			/**
			 * Open the customer.out file for read.
			 */
			fr = new FileReader(Constant.CUSTOMER_PROFILE_FILE_NAME);
			BufferedReader br = new BufferedReader(fr);
			String jsonTxt = null;
			// String password =
			// StringUtil.getMessageDigest(Constant.DEMO_PASSWORD);
			String password = Constant.DEMO_PASSWORD;
			CustomerTO custTO = null;
			int count = 1;

			/**
			 * Loop through the file until EOF. Save the content of each row in
			 * the jsonTxt string.
			 */
			while ((jsonTxt = br.readLine()) != null) {

				if (jsonTxt.trim().length() == 0)
					continue;

				try {
					/**
					 * Construct the CustomerTO by passing the jsonTxt as an
					 * input argument to its constructor. If the jsonTxt can be
					 * deserialized into CustomerTO then a valid object will be
					 * returned but if it fails to desiralize it for any reason
					 * the null pointer will be returned.
					 */
					custTO = new CustomerTO(jsonTxt.trim());

					// Set password to each CutomerTO
					custTO.setPassword(password);
				} catch (Exception e) {
					System.out
							.println("ERROR: Not able to parse the json string: \t"
									+ jsonTxt);
					e.printStackTrace();
				}

				/**
				 * Make sure that custTO is not null, which means the jsonTxt
				 * read from the customer.out was successfully converted into
				 * CustomerTO object.
				 */
				if (custTO != null) {

					/**
					 * Persist user-profile information into kv-store. All the
					 * Customer specific read/write operations are defined in
					 * CustomerDAO class.
					 */
					customerDao.insert(custTO);

					/**
					 * If username & password doesn't exist already in the
					 * kv-store then the new profile will be created and the
					 * 'version' object would have the Version of the new
					 * key-value pair, but if the profile already exist in the
					 * store with the same credential then null will be returned
					 * and exception can be handled appropriately.
					 */
					System.out.println(count++ + " " + custTO.getJsonTxt());

				} // EOF if

			} // EOF while
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			fr.close();
		}
	}
	public void loadCastData() throws IOException
	{
		FileReader fr = null;
		Version version = null;
		CastDao castDao = new CastDao();
        
		try {

			/**
			 * Open the customer.out file for read.
			 */
			
			fr = new FileReader(Constant.MOVIE_CASTS_FILE_NAME);
			BufferedReader br = new BufferedReader(fr);
			String jsonTxt = null;
			
			CastTO castTO = null;
			int count = 1;

			/**
			 * Loop through the file until EOF. Save the content of each row in
			 * the jsonTxt string.
			 */
			while ((jsonTxt = br.readLine()) != null) {
				if (jsonTxt.trim().length() == 0)
					continue;

				try {
					/**
					 * Construct the CustomerTO by passing the jsonTxt as an
					 * input argument to its constructor. If the jsonTxt can be
					 * deserialized into CustomerTO then a valid object will be
					 * returned but if it fails to desiralize it for any reason
					 * the null pointer will be returned.
					 */
					castTO = new CastTO(jsonTxt.trim());

					// Set password to each CutomerTO
				} catch (Exception e) {
					System.out
							.println("ERROR: Not able to parse the json string: \t"
									+ jsonTxt);
					e.printStackTrace();
				}

				/**
				 * Make sure that custTO is not null, which means the jsonTxt
				 * read from the customer.out was successfully converted into
				 * CustomerTO object.
				 */
				if (castTO != null) {

					/**
					 * Persist user-profile information into kv-store. All the
					 * Customer specific read/write operations are defined in
					 * CustomerDAO class.
					 */
					castDao.insert(castTO);

					/**
					 * If username & password doesn't exist already in the
					 * kv-store then the new profile will be created and the
					 * 'version' object would have the Version of the new
					 * key-value pair, but if the profile already exist in the
					 * store with the same credential then null will be returned
					 * and exception can be handled appropriately.
					 */
					System.out.println(count++ + " " + castTO.getJsonTxt());

				} // EOF if

			} // EOF while
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			fr.close();
		}
	}
	public void loadCrewData() throws IOException
	{
		FileReader fr = null;
		Version version = null;
		CrewDao crewDao = new CrewDao();
        
		try {

			/**
			 * Open the customer.out file for read.
			 */
			
			fr = new FileReader(Constant.MOVIE_CREW_FILE_NAME);
			BufferedReader br = new BufferedReader(fr);
			String jsonTxt = null;
			
			CrewTO crewTO = null;
			int count = 1;

			/**
			 * Loop through the file until EOF. Save the content of each row in
			 * the jsonTxt string.
			 */
			while ((jsonTxt = br.readLine()) != null) {
				if (jsonTxt.trim().length() == 0)
					continue;

				try {
					/**
					 * Construct the CustomerTO by passing the jsonTxt as an
					 * input argument to its constructor. If the jsonTxt can be
					 * deserialized into CustomerTO then a valid object will be
					 * returned but if it fails to desiralize it for any reason
					 * the null pointer will be returned.
					 */
					crewTO = new CrewTO(jsonTxt.trim());
					// Set password to each CutomerTO
				} catch (Exception e) {
					System.out
							.println("ERROR: Not able to parse the json string: \t"
									+ jsonTxt);
					e.printStackTrace();
				}

				/**
				 * Make sure that custTO is not null, which means the jsonTxt
				 * read from the customer.out was successfully converted into
				 * CustomerTO object.
				 */
				if (crewTO != null) {

					/**
					 * Persist user-profile information into kv-store. All the
					 * Customer specific read/write operations are defined in
					 * CustomerDAO class.
					 */
					crewDao.insert(crewTO);

					/**
					 * If username & password doesn't exist already in the
					 * kv-store then the new profile will be created and the
					 * 'version' object would have the Version of the new
					 * key-value pair, but if the profile already exist in the
					 * store with the same credential then null will be returned
					 * and exception can be handled appropriately.
					 */
					System.out.println(count++ + " " + crewTO.getCrewJson());

				} // EOF if

			} // EOF while
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			fr.close();
		}
	}
	
	public void loadMovieData() throws IOException
	{
		FileReader fr = null;
		Version version = null;
		MovieDao movieDao = new MovieDao();
        
		try {

			/**
			 * Open the customer.out file for read.
			 */
			
			fr = new FileReader(Constant.MOVIE_INFO_FILE_NAME);
			BufferedReader br = new BufferedReader(fr);
			String jsonTxt = null;
			
			MovieTO movieTO = null;
			int count = 1;

			/**
			 * Loop through the file until EOF. Save the content of each row in
			 * the jsonTxt string.
			 */
			while ((jsonTxt = br.readLine()) != null) {
				if (jsonTxt.trim().length() == 0)
					continue;

				try {
					/**
					 * Construct the CustomerTO by passing the jsonTxt as an
					 * input argument to its constructor. If the jsonTxt can be
					 * deserialized into CustomerTO then a valid object will be
					 * returned but if it fails to desiralize it for any reason
					 * the null pointer will be returned.
					 */
					movieTO = new MovieTO(jsonTxt.trim());

					// Set password to each CutomerTO
				} catch (Exception e) {
					System.out
							.println("ERROR: Not able to parse the json string: \t"
									+ jsonTxt);
					e.printStackTrace();
				}

				/**
				 * Make sure that custTO is not null, which means the jsonTxt
				 * read from the customer.out was successfully converted into
				 * CustomerTO object.
				 */
				if (movieTO != null) {

					/**
					 * Persist user-profile information into kv-store. All the
					 * Customer specific read/write operations are defined in
					 * CustomerDAO class.
					 */
					movieDao.insert(movieTO);
					count++;

					/**
					 * If username & password doesn't exist already in the
					 * kv-store then the new profile will be created and the
					 * 'version' object would have the Version of the new
					 * key-value pair, but if the profile already exist in the
					 * store with the same credential then null will be returned
					 * and exception can be handled appropriately.
					 */
					System.out.println(count++ + " " + movieTO.getMovieJsonTxt());

				} // EOF if

			} // EOF while
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			fr.close();
		}
	}
	public void loadGenreData()
	{
		PrimaryKey key = null;
		Table table =getKVStore().getTableAPI().getTable("genre");
		key = table.createPrimaryKey();
		int i=1;
		TableIterator<Row> rows = getKVStore().getTableAPI().tableIterator(key, null, null);
		while(rows.hasNext())
		{
			GenreDao genreDao = new GenreDao();
			String jsonStr =rows.next().toJsonString(true);
			GenreTO genreTO = new GenreTO(jsonStr);
			System.out.println(i+"Genre:"+genreTO.getId()+":"+genreTO.toJsonString());
			genreDao.insert(genreTO);
			i++;
		}
		
	}
	public void loadCustomerGenreData()
	{
		PrimaryKey key = null;
		int i=1;
		Table table =getKVStore().getTableAPI().getTable("customer.customerGenres");
		key = table.createPrimaryKey();
		TableIterator<Row> rows = getKVStore().getTableAPI().tableIterator(key, null, null);
		while(rows.hasNext())
		{
			CustomerDao customerDao = new CustomerDao();
			String jsonStr =rows.next().toJsonString(true);
			CustomerGenreTO customerGenreTO = new CustomerGenreTO(jsonStr);
			System.out.println(i+":Genre:"+customerGenreTO.getId()+":"+customerGenreTO.toJsonString());
			customerDao.insertMovies4CustomerByGenre(customerGenreTO);
			i++;
		}
	}
	public void loadCustMovieGenre()
	{
		PrimaryKey key = null;
		int i=1;
		Table table =getKVStore().getTableAPI().getTable("customer.customerGenreMovie");
		key = table.createPrimaryKey();
		TableIterator<Row> rows = getKVStore().getTableAPI().tableIterator(key, null, null);
		while(rows.hasNext())
		{
			GenreDao genreDao = new GenreDao();
			String jsonStr =rows.next().toJsonString(true);
			CustomerGenreMovieTO customerGenreMovieTO = new CustomerGenreMovieTO(jsonStr);
			System.out.println(i+":customerGenreMovieTO:"+customerGenreMovieTO.getId()+":"+customerGenreMovieTO.toJsonString());
			genreDao.insertCustGenreMovie(customerGenreMovieTO);
			i++;
		}
	}

}

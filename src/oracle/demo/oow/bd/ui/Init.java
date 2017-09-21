package oracle.demo.oow.bd.ui;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import oracle.demo.oow.bd.util.FileWriterUtil;
import oracle.demo.oow.hb.dao.ConstantsHBase;

public class Init extends HttpServlet {

	
	/**
	 * Initialization of the servlet. <br>
	 *
	 * @throws ServletException if an error occurs
	 */
	public void init(ServletConfig config) throws ServletException {
		// Put your code here
		super.init(config);
		String path;
		FileInputStream fileInputStream;
		Properties p = new Properties();
		String prefix = Init.class.getClass().getResource("/").getPath();
		System.out.println("prefix:"+prefix);
		path=prefix.substring(1, prefix.indexOf("classes"));
		try {
			fileInputStream = new FileInputStream(prefix+"movie.properties");
			p.load(fileInputStream);
			String out_file=p.getProperty("output_file");
			if(out_file!=null)
			{
				FileWriterUtil.OUTPUT_FILE=out_file;
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

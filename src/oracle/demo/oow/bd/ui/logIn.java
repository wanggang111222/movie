package oracle.demo.oow.bd.ui;

import java.io.IOException;
import java.io.PrintWriter;

import java.util.Date;

import javax.servlet.*;
import javax.servlet.http.*;



import oracle.demo.oow.bd.dao.BaseDAO;
import oracle.demo.oow.bd.pojo.ActivityType;
import oracle.demo.oow.bd.to.ActivityTO;
import oracle.demo.oow.bd.to.CustomerTO;
import oracle.demo.oow.hb.dao.ActivityDao;
import oracle.demo.oow.hb.dao.CustomerDao;
import oracle.kv.KVStore;

public class logIn extends HttpServlet {
    //private static final String CONTENT_TYPE = "text/html; charset=windows-1252";
    private String loginPage = "login.jsp";
    private String indexPage = "index.jsp";

    public void init(ServletConfig config) throws ServletException {
        super.init(config);
    }

    public void doGet(HttpServletRequest request,
                      HttpServletResponse response) throws ServletException,
                                                           IOException {
        doPost(request, response);
    }

    public void doPost(HttpServletRequest request,
                       HttpServletResponse response) throws ServletException,
                                                            IOException {
       // BaseDAO base = new BaseDAO();
        
        //KVStore store = null;
        String message =
            "<h1>Please make sure your Oracle NoSQL Database instance is up and running</h1>";
        PrintWriter out = response.getWriter();

        try {
            //store = base.getKVStore();

                String username = request.getParameter("username");
                String password = request.getParameter("password");
                boolean useMoviePosters = request.getParameter("useMoviePosters") == null? false : true;
                System.out.println(11);
                CustomerDao cdao = new CustomerDao();
                CustomerTO cto =
                    cdao.getCustomerByCredential(username, password);
                Date date = new Date();

                if (cto != null) {

                    // Delete all the previous ratings of the customers from the DB
                    
                    /////// ACTIVITY ////////
                    ActivityTO activityTO = new ActivityTO();
                    activityTO.setActivity(ActivityType.LOGIN);
                    activityTO.setCustId(cto.getId());
                    ActivityDao aDAO = new ActivityDao();
                    aDAO.insertCustomerActivity(activityTO);
                    
                    activityTO.setActivity(ActivityType.LIST_MOVIES);
                    aDAO.insertCustomerActivity(activityTO);
                    
                    HttpSession session = request.getSession();
                    session.setAttribute("username", username);
                    session.setAttribute("time", date);
                    session.setAttribute("userId", cto.getId());
                    session.setAttribute("name", cto.getName());
                    session.setAttribute("useMoviePosters", useMoviePosters);

                    //Ashok
                    System.out.println(" setting session and redirecting " + activityTO.toJsonString());
                    response.sendRedirect(indexPage);
                    System.out.println("Ìø×ªµ½loginout.jsp");
                } else {
                    response.sendRedirect(loginPage + "?error=1");
                }

        } catch (Exception e) {
            e.printStackTrace();
        } //try/catch


    }
}

package sparqlExecute;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import org.apache.jena.query.*;

public class SparqlExecute {
	static String sparqlQueryString;
	static String SPARQL_SERVICE = "http://dbpedia.org/sparql";
	static String URL = "jdbc:postgresql://spacia.db.ics.keio.ac.jp/sparql_tmp_database?useUnicode=true&charcterEncoding=utf8";

	public static void main(String[] args) throws IOException {
		List<String> collum = new ArrayList<String>();
		
		org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.OFF);
		Scanner scan = new Scanner(System.in);
		do {
			System.out.println("enter a sparql query");
			sparqlQueryString = scan.nextLine();
			try {
				Query query = QueryFactory.create(sparqlQueryString);
				QueryExecution qexec = QueryExecutionFactory
						.sparqlService(SPARQL_SERVICE, query);
				ResultSet rs = qexec.execSelect();
				QuerySolution qs = rs.next();
				Iterator<String> iterator = qs.varNames();//カラムの順番がなぜか変
				while(iterator.hasNext()){
					collum.add(iterator.next());
					System.out.println(collum);
				}
				System.out.println(collum.get(0));//population
				System.out.println(collum.get(1));
				System.out.println(collum.get(2));
				System.out.println(qs.getLiteral(collum.get(0)));//316
				System.out.println(qs.getLiteral(collum.get(1)));
				System.out.println(qs.getLiteral(collum.get(2)));
				System.out.println(qs);
				
			   //テーブル名に使用する日付を取得
            Date date = new Date();
            SimpleDateFormat sdf = new SimpleDateFormat("yyMMddHHmmss");
            String tmpdate = sdf.format(date);
			
            //create文の作成と実行
				String sql = "CREATE TABLE sample_" + tmpdate + "(";
				for(int i = 0; i < collum.size()-1; i++){
					sql += collum.get(i) +" text, ";
				}
				sql += collum.get(collum.size()-1) +" text";
				sql += ");";
				System.out.println(sql);
				Connection connection = DriverManager.getConnection
						(URL,"machida", "machida");
				Statement statement = connection.createStatement();
				statement.execute(sql);
				
				
				//insert文の作成と実行
				//1個目
				sql = "INSERT INTO sample_" + tmpdate + "(";
				for(int i = 0; i < collum.size()-1; i++){
					sql += collum.get(i) + ",";
				}
				sql += collum.get(collum.size()-1) + ")VALUES(";
				for(int i = 0; i < collum.size()-1; i++){
					sql += "'" + qs.getLiteral(collum.get(i)) + "',";
				}
				sql += "'" + qs.getLiteral(collum.get(collum.size()-1)) + "');";
				System.out.println("79:"+sql);
				statement.executeUpdate(sql);
				
				
				//2個目以降
				while(rs.hasNext()){
					qs = rs.next();
					iterator = qs.varNames();
					collum.clear();
					while(iterator.hasNext()){
						collum.add(iterator.next());
						System.out.println(collum);
					}	
					sql = "INSERT INTO sample_" + tmpdate + "(";
					for(int i = 0; i < collum.size()-1; i++){
						sql += collum.get(i) + ",";
					}
					sql += collum.get(collum.size()-1) + ")VALUES(";
					for(int i = 0; i < collum.size()-1; i++){
						sql += "'" + qs.getLiteral(collum.get(i)) + "',";
					}
					sql += "'" + qs.getLiteral(collum.get(collum.size()-1)) + "');";
					System.out.println(sql);
					statement.executeUpdate(sql);
				}
				
				
				
				
			} catch (Exception e) {
				e.printStackTrace();
				// TODO: handle exception
			}
		} while (true);

	}
}

package Hadoop.hbase;
//test
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.*;

import org.apache.hadoop.util.LineReader;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class App
{       
     //IP Map Process Start **********************************************************************************************************
     public static String JsonObjtoIP(String JsonString){       
          String JsonStr = JsonString;
          String client_ip_str;  
          try{       
               JSONObject jsonobj = JSON. parseObject(JsonStr);
              client_ip_str = JSON. toJSONString(jsonobj.get("clientip"));
               return client_ip_str;       
          } catch (Exception e) {
               e.printStackTrace();
          }  
          return "Error" ;
     }
  
     public static class IPMapper extends TableMapper <Text, IntWritable> {
          public static final byte[] family = "data" .getBytes();
          public static final byte[] qualifier = "json" .getBytes();  
          private final IntWritable ONE = new IntWritable(1);
          private Text text = new Text();          
        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
             String val = new String(value.getValue(family, qualifier));
             String ip  = JsonObjtoIP(val);
             text.set(ip);
             context.write( text, ONE);  
        }
     }
     //IP Map Process End *************************************************************************************************************
  
     //Request Map Process Start ******************************************************************************************************
     public static String JsonObjtoRequest(String JsonString){            
          String JsonStr = JsonString;
          String client_Request_str;            
          try{                 
               JSONObject jsonobj = JSON. parseObject(JsonStr);
               client_Request_str = JSON. toJSONString(jsonobj.get("request"));
               return client_Request_str;                 
          } catch (Exception e) {
               e.printStackTrace();
          }  
          return "Error" ;
     }
       
     public static class RequestMapper extends TableMapper <Text, IntWritable> {
          public static final byte[] family = "data" .getBytes();
          public static final byte[] qualifier = "json" .getBytes();       
          private final IntWritable ONE = new IntWritable(1);
          private Text text = new Text();          
        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
             String val = new String(value.getValue(family, qualifier));
             String Request  = JsonObjtoRequest(val);
             text.set(Request);
             context.write( text, ONE);  
        }  
     }
     //Request Map Process End ******************************************************************************************************
    
     //Agent Map Process Start ******************************************************************************************************
     public static String JsonObjtoAgent(String JsonString){            
          String JsonStr = JsonString;
          String client_Agent_str;            
          try{                 
               JSONObject jsonobj = JSON. parseObject(JsonStr);
               client_Agent_str = JSON. toJSONString(jsonobj.get("agent"));
               return client_Agent_str;                 
          } catch (Exception e) {
               e.printStackTrace();
          }  
          return "Error" ;
     }
       
     public static class AgentMapper extends TableMapper <Text, IntWritable> {
          public static final byte[] family = "data" .getBytes();
          public static final byte[] qualifier = "json" .getBytes();       
          private final IntWritable ONE = new IntWritable(1);
          private Text text = new Text();          
        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
             String val = new String(value.getValue(family, qualifier));
             String Agent  = JsonObjtoAgent(val);
             text.set(Agent);
             context.write( text, ONE);  
        }  
     }
     //Agent Map Process End ******************************************************************************************************
     
     //Referrer_Host Map Process Start ********************************************************************************************
     public static String GetURLHost(String url){
 	    if(url == null || url.length() == 0)
 	        return "";

 	    int doubleslash = url.indexOf("//");
 	    if(doubleslash == -1)
 	        doubleslash = 0;
 	    else
 	        doubleslash += 2;

 	    int end = url.indexOf('/', doubleslash);
 	    end = end >= 0 ? end : url.length();

 	    int port = url.indexOf(':', doubleslash);
 	    end = (port > 0 && port < end) ? port : end;

 	    return url.substring(doubleslash, end);
 	 }
     
     public static String JsonObjtoReferrerHost(String JsonString){            
          String JsonStr = JsonString;
          String client_Referrer_str; 
          String Host;
          try{                 
               JSONObject jsonobj = JSON. parseObject(JsonStr);
               client_Referrer_str = JSON. toJSONString(jsonobj.get("referrer"));
               client_Referrer_str = client_Referrer_str.replace("\"\\\"", "");
               client_Referrer_str = client_Referrer_str.replace("\\\"\"", "");
               Host = GetURLHost(client_Referrer_str);
               return Host;                 
          } catch (Exception e) {
               e.printStackTrace();
          }  
          return "Error" ;
     }
         
     public static class ReferrerHostMapper extends TableMapper <Text, IntWritable> {
          public static final byte[] family = "data".getBytes();
          public static final byte[] qualifier = "json".getBytes();       
          private final IntWritable ONE = new IntWritable(1);
          private Text text = new Text();          
        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
             String val = new String(value.getValue(family, qualifier));
             String ReferrerHost = JsonObjtoReferrerHost(val); 
             text.set(ReferrerHost);
             context.write(text, ONE);  
        }  
     }
     //Referrer_Host Map Process End ***************************************************************************************************
     
     //KeyWords Map Process Start ******************************************************************************************************
 	  public static String GetSearchKeyWords(String strQuery) {
		 //x 定义了匹配的正则表达式，目前仅匹配Google的英文搜索词
 		  String x = "q=(?!u)\\w*\\.\\w*\\b";
          Pattern pattern = Pattern.compile(x);
          String s = " NULL";
          Matcher matcher = pattern.matcher(strQuery);
          while (matcher.find()){
            s = matcher.group();
            s = s.substring(1, s.length());
            s = s.replace("=", "");
          }
        return s;
	  }
       
 	  public static String JsonObjtoReferrer(String JsonString){            
         String JsonStr = JsonString;
         String client_Referrer_str; 
         try{                 
              JSONObject jsonobj = JSON. parseObject(JsonStr);
              client_Referrer_str = JSON. toJSONString(jsonobj.get("referrer"));
              client_Referrer_str = client_Referrer_str.replace("\"\\\"", "");
              client_Referrer_str = client_Referrer_str.replace("\\\"\"", "");
              return client_Referrer_str;                 
         } catch (Exception e) {
              e.printStackTrace();
         }  
         return "Error" ;
      }
 	  
 	  public static String JsonObjtoSearchKeyWords(String JsonString){  
 		  String Host = JsonObjtoReferrerHost(JsonString);
 		  String Referrer = JsonObjtoReferrer(JsonString);
 		  String KeyWords = "";                
          if(Host.indexOf("google") != -1){
        	  KeyWords = GetSearchKeyWords(Referrer);
        	  return KeyWords;
     		}
         return "NULL" ;
      }
 	  
      public static class KeyWordsMapper extends TableMapper <Text, IntWritable> {
          public static final byte[] family = "data".getBytes();
          public static final byte[] qualifier = "json".getBytes();       
          private final IntWritable ONE = new IntWritable(1);
          private Text text = new Text();          
        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
             String val = new String(value.getValue(family, qualifier));
             String SearchKeyWords = JsonObjtoSearchKeyWords(val); 
             text.set(SearchKeyWords);
             context.write(text, ONE);  
        }  
      }
      //KeyWords Map Process End ******************************************************************************************************
  
     public static class LogReducer extends Reducer<Text, IntWritable, Text, IntWritable>  {
          public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
               int i = 0;
               for (IntWritable val : values) {
                    i += val.get();
               }
               context.write(key, new IntWritable(i));
          }
     }
      
     public static String LeaveOnlyOneSpace(char[] chars) {
         int finalIndex = 0;
         int spaceCount = 0;

         for (int i = 0; i < chars.length; ++i) {
             if (chars[i] != ' ') {
                 chars[finalIndex] = chars[i];
                 ++finalIndex;
                 spaceCount = 0;
             } else if (chars[i] == ' ' && finalIndex == 0) {

             } else if (chars[i] == ' ' && spaceCount == 0) {
                 chars[finalIndex] = chars[i];
                 ++finalIndex;
                 ++spaceCount;
             } else if (chars[i] == ' ' && spaceCount > 0) {
                 ++spaceCount;
             }
         }

         for (int i = finalIndex; i < chars.length; ++i) {
             chars[i] = ' ';
         }
         return (String.valueOf(chars));
     }
 	
 	 static String ReplaceTab(String str) {
          String dest = "";
          if (str!= null ) {
        	  Pattern p = Pattern. compile("\t|\r|\n");
              Matcher m = p.matcher(str);
              dest = m.replaceAll( " ");
         }
        return dest;
     }
 	 
     static String ReplaceBlank(String str) {
         String dest = "";
         if (str!= null ) {
                Pattern p = Pattern. compile("\\s*|\t|\r|\n");
              Matcher m = p.matcher(str);
              dest = m.replaceAll( "");
         }
        return dest;
      }

      public static String GetDate() {
         SimpleDateFormat lFormat;
          Date date = null;
          Calendar MyDate = Calendar. getInstance();
          MyDate.setTime( new java.util.Date());
          date = MyDate.getTime();
          lFormat =  new SimpleDateFormat("yyyyMMdd" );
          String gRtnStr = lFormat.format(date);
          return gRtnStr;
      }
      
      public static long date2long(String datestr, String format) {
    	  try {
              SimpleDateFormat sdf = new SimpleDateFormat(format);
              Date date = sdf.parse(datestr);
              return date.getTime(); 
          } catch (ParseException e) {
            e.printStackTrace();
          }
        return 0;
      } 
      
      public static void Hdfstosql(String str_filepath,String str_table,String str_type) throws Exception{
        
         Configuration conf_fs = new Configuration();
          FileSystem fs=FileSystem. get(conf_fs);
        
          String inputPath = str_filepath;
          Path IP_FilePath = new Path(inputPath);
        
          System. out.print("Start Data Analysis..." + "\n" );
       
          FSDataInputStream dis = fs.open(IP_FilePath);
          LineReader in = new LineReader(dis,conf_fs);
          Text line = new Text();
          String str_data,str_count,str_tmp = "";
          int int_start,int_end,int_Blank = 0;
       
          Connection con_MySQL = null;
          Statement stmt;
    
          try {
                Class. forName("com.mysql.jdbc.Driver").newInstance();
              con_MySQL = DriverManager.getConnection("jdbc:mysql://localhost:3306/Logstash", "root", "" );
          }
          catch (Exception e) {
              System. out.print("MYSQL ERROR:" + e.getMessage() + "\n");
              return ;
          }
        
          if("IP" == str_type || "Request" == str_type){
               while(in.readLine(line) > 0){
                  str_tmp = line.toString();
                  str_tmp = ReplaceBlank(str_tmp);
                  int_start = str_tmp.indexOf( "\"");
                  int_end = str_tmp.lastIndexOf( "\"");
                  if(-1 == int_start || -1 == int_end)
                       continue ;
                  str_data = str_tmp.substring(int_start, int_end);
                  str_data = str_data.replaceAll( "\"", "" );
                  str_count = str_tmp.substring((int_end+1), str_tmp.length());
                
                  System. out.print(str_type + " Data Transmission from HDFS to MySQL..." + "\n" );
                
                  try{
                        stmt = con_MySQL.createStatement();
                        stmt.executeUpdate( "INSERT INTO " + str_table + "(" + str_type + ",Counter) VALUES (" + "'" + str_data + "'" + "," + "'" + str_count + "'" + ")");   
                  }
                  catch (Exception e) {
                      System.out.print("MYSQL ERROR:" + e.getMessage() + "\n");
                      System.out.print("INSERT INTO " + str_table + "(" + str_type + ",Counter) VALUES (" + "'" + str_data + "'" + "," + "'" + str_count + "'" + ")" + "\n");
                  }    
              }
              con_MySQL.close();
              dis.close();
              in.close();
              return ;
          }
         
          if("Agent" == str_type){
               while(in.readLine(line) > 0){
                  str_tmp = line.toString();
                  str_tmp = str_tmp.replace("\\\"", "");
                  str_tmp = ReplaceBlank(str_tmp);
                  int_start = str_tmp.indexOf( "\"");
                  int_end = str_tmp.lastIndexOf( "\"");
                  if(-1 == int_start || -1 == int_end)
                       continue ;
                  str_data = str_tmp.substring(int_start, int_end);
                  str_data = str_data.replaceAll( "\"", "" );
                  str_count = str_tmp.substring((int_end+1), str_tmp.length());
                  System. out.print(str_type + " Data Transmission from HDFS to MySQL..." + "\n" );
                
                  try{
                        stmt = con_MySQL.createStatement();
                        stmt.executeUpdate( "INSERT INTO " + str_table + "(" + str_type + ",Counter) VALUES (" + "'" + str_data + "'" + "," + "'" + str_count + "'" + ")");   
                  }
                  catch (Exception e) {
                      System.out.print("MYSQL ERROR:" + e.getMessage() + "\n");
                      System.out.print("INSERT INTO " + str_table + "(" + str_type + ",Counter) VALUES (" + "'" + str_data + "'" + "," + "'" + str_count + "'" + ")" + "\n");
                  }    
              }
              con_MySQL.close();
              dis.close();
              in.close();
              return ;
          }
          
          if("ReferrerHost" == str_type || "SearchKeyWords" == str_type){
              while(in.readLine(line) > 0){
                 str_tmp = line.toString();
                 str_tmp = ReplaceTab(str_tmp);
                 char[] tmp_chars = str_tmp.toCharArray();
                 str_tmp = LeaveOnlyOneSpace(tmp_chars);
                 int_Blank = str_tmp.indexOf(" ");
                 if(-1 == int_Blank)
                      continue ;
                 str_data = str_tmp.substring(0,int_Blank);
                 str_data = ReplaceBlank(str_data);
                 str_count = str_tmp.substring(int_Blank, str_tmp.length());
                 str_count = ReplaceBlank(str_count);
                 System. out.print(str_type + " Data Transmission from HDFS to MySQL..." + "\n" );
               
                 try{
                       stmt = con_MySQL.createStatement();
                       stmt.executeUpdate( "INSERT INTO " + str_table + "(" + str_type + ",Counter) VALUES (" + "'" + str_data + "'" + "," + "'" + str_count + "'" + ")");   
                 }
                 catch (Exception e) {
                     System.out.print("MYSQL ERROR:" + e.getMessage() + "\n");
                     System.out.print("INSERT INTO " + str_table + "(" + str_type + ",Counter) VALUES (" + "'" + str_data + "'" + "," + "'" + str_count + "'" + ")" + "\n");
                 }    
             }
             con_MySQL.close();
             dis.close();
             in.close();
             return ;
         }
      }
    
     public static void main(String[] args) throws Exception {
         
          if(args.length != 2){
               System. out.println("参数错误" );
               System. out.println("起始日期   截至日期   格式：2013年01月01日00:00:00" );
               return ;
          }
       
          System. out.println ("启动计算节点数 ：6" );
          System. out.println ("Scan Start : " + args[0]);
          System. out.println ("Scan End   : " + args[1]);
       
         String format= "yyyy年MM月dd日HH:mm:ss" ;
          long StartRow = date2long(args[0], format);
          long StopRow  = date2long(args[1], format);

          String tablename  = "logstash";
          Configuration conf_fs = new Configuration();
          Configuration conf_hbase = HBaseConfiguration. create();
     
        //Start IP Job Config *********************************************************************************************************
        Job job_ip = Job. getInstance(conf_hbase);
        job_ip.setJobName( "IP-Count");
        job_ip.setJarByClass(App. class);
  
        Scan scan_ip = new Scan(Bytes.toBytes(StartRow),Bytes. toBytes(StopRow));
        scan_ip.setCaching(500);
        scan_ip.setCacheBlocks( false);
     
        TableMapReduceUtil.initTableMapperJob(tablename, scan_ip, IPMapper. class, Text.class, IntWritable.class, job_ip);
     
        job_ip.setReducerClass(LogReducer. class);
        job_ip.setNumReduceTasks(6);
      
        FileSystem fs=FileSystem. get(conf_fs);
        if(fs.exists(new Path("/Logstash/IP_Count_File"))){
            fs.delete( new Path("/Logstash/IP_Count_File" ),true);
         }
     
        FileOutputFormat.setOutputPath(job_ip, new Path("/Logstash/IP_Count_File" ));
        //End IP Job Config *************************************************************************************************************
     
        //Start Request Job Config ******************************************************************************************************
        Job job_Request = Job. getInstance(conf_hbase);
        job_Request.setJobName( "Request-Count");
        job_Request.setJarByClass(App. class);
  
        Scan scan_Request = new Scan(Bytes.toBytes(StartRow),Bytes.toBytes(StopRow));
        scan_Request.setCaching(500);
        scan_Request.setCacheBlocks( false);
     
        TableMapReduceUtil.initTableMapperJob(tablename, scan_Request, RequestMapper.class , Text.class, IntWritable.class, job_Request);
     
        job_Request.setReducerClass(LogReducer. class);
        job_Request.setNumReduceTasks(6);
     
        if(fs.exists(new Path("/Logstash/Request_Count_File"))){
            fs.delete( new Path("/Logstash/Request_Count_File" ),true);
         }
     
        FileOutputFormat.setOutputPath(job_Request, new Path("/Logstash/Request_Count_File" ));
        //End Request Job Config *********************************************************************************************************
       
        //Start Agent Job Config *********************************************************************************************************
        Job job_Agent = Job. getInstance(conf_hbase);
        job_Agent.setJobName( "Agent-Count");
        job_Agent.setJarByClass(App. class);
  
        Scan scan_Agent = new Scan(Bytes.toBytes(StartRow),Bytes.toBytes(StopRow));
        scan_Agent.setCaching(500);
        scan_Agent.setCacheBlocks( false);
     
        TableMapReduceUtil.initTableMapperJob(tablename, scan_Agent, AgentMapper.class , Text.class, IntWritable.class, job_Agent);
     
        job_Agent.setReducerClass(LogReducer. class);
        job_Agent.setNumReduceTasks(6);
     
        if(fs.exists(new Path("/Logstash/Agent_Count_File"))){
            fs.delete( new Path("/Logstash/Agent_Count_File"),true);
         }
     
        FileOutputFormat.setOutputPath(job_Agent, new Path("/Logstash/Agent_Count_File"));
        //End Agent Job Config ************************************************************************************************************
        
        //Start Referrer_Host Job Config **************************************************************************************************
        Job job_Referrer_Host = Job.getInstance(conf_hbase);
        job_Referrer_Host.setJobName("Host-Count");
        job_Referrer_Host.setJarByClass(App.class);
  
        Scan scan_Referrer_Host = new Scan(Bytes.toBytes(StartRow),Bytes.toBytes(StopRow));
        scan_Referrer_Host.setCaching(500);
        scan_Referrer_Host.setCacheBlocks(false);
     
        TableMapReduceUtil.initTableMapperJob(tablename, scan_Referrer_Host, ReferrerHostMapper.class , Text.class, IntWritable.class, job_Referrer_Host);
     
        job_Referrer_Host.setReducerClass(LogReducer.class);
        job_Referrer_Host.setNumReduceTasks(6);
     
        if(fs.exists(new Path("/Logstash/ReferrerHost_Count_File"))){
            fs.delete( new Path("/Logstash/ReferrerHost_Count_File"),true);
         }
     
        FileOutputFormat.setOutputPath(job_Referrer_Host, new Path("/Logstash/ReferrerHost_Count_File"));
        //End Referrer_Host Job Config ************************************************************************************************************
   
        //Start SearchKeyWords Job Config *********************************************************************************************************
        Job job_KeyWords = Job.getInstance(conf_hbase);
        job_KeyWords.setJobName("KeyWords-Count");
        job_KeyWords.setJarByClass(App.class);
  
        Scan scan_KeyWords = new Scan(Bytes.toBytes(StartRow),Bytes.toBytes(StopRow));
        scan_KeyWords.setCaching(500);
        scan_KeyWords.setCacheBlocks(false);
     
        TableMapReduceUtil.initTableMapperJob(tablename, scan_KeyWords, KeyWordsMapper.class , Text.class, IntWritable.class, job_KeyWords);
     
        job_KeyWords.setReducerClass(LogReducer.class);
        job_KeyWords.setNumReduceTasks(6);
     
        if(fs.exists(new Path("/Logstash/KeyWords_File"))){
            fs.delete( new Path("/Logstash/KeyWords_File"),true);
         }
     
        FileOutputFormat.setOutputPath(job_KeyWords, new Path("/Logstash/KeyWords_File"));
        //End SearchKeyWords Job Config ************************************************************************************************************
        
        job_ip.submit();
        job_Agent.submit();
        job_Request.submit();
        job_KeyWords.submit();
        job_Referrer_Host.submit();
        
        boolean bln_ip = job_ip.isComplete();
        boolean bln_Agent = job_Agent.isComplete();
        boolean bln_Request = job_Request.isComplete();
        boolean bln_KeyWords = job_KeyWords.isComplete();
        boolean bln_ReferrerHost = job_Referrer_Host.isComplete();
      
        while (!bln_ip || !bln_Request || !bln_Agent || !bln_ReferrerHost || !bln_KeyWords) {
              bln_ip = job_ip.isComplete();
              bln_Agent = job_ip.isComplete();
              bln_Request = job_Request.isComplete();
              bln_KeyWords = job_KeyWords.isComplete();
              bln_ReferrerHost = job_Referrer_Host.isComplete();
        }
        
        //HDFD to MySQL ********************************************************************************************************************
        String str_table_name = "";
        str_table_name = GetDate();
      
        System. out.print("Database Initialize ..." + "\n" );
        Connection con_MySQL = null;
        Statement stmt;
  
        try {           
           Class. forName("com.mysql.jdbc.Driver").newInstance();
            con_MySQL = DriverManager.getConnection("jdbc:mysql://localhost:3306/Logstash", "root", "" );
        }
        catch (Exception e) {
            System. out.print("MYSQL ERROR:" + e.getMessage() + "\n");
            return ;
        }
        try{
           stmt = con_MySQL.createStatement();
            stmt.executeUpdate( "DROP TABLE " + str_table_name + "_Agent" ); 
            stmt.executeUpdate( "DROP TABLE " + str_table_name + "_IP" );
            stmt.executeUpdate( "DROP TABLE " + str_table_name + "_Request" );
            stmt.executeUpdate( "DROP TABLE " + str_table_name + "_ReferrerHost" );
            stmt.executeUpdate( "DROP TABLE " + str_table_name + "_SearchKeyWords" );
        }
        catch (Exception e) {
        }
        try{
           stmt = con_MySQL.createStatement();
            stmt.executeUpdate( "CREATE TABLE " + str_table_name + "_Agent   ( Agent   varchar(2048),Counter int )");    
            stmt.executeUpdate( "CREATE TABLE " + str_table_name + "_IP      ( IP      varchar(  20),Counter int )");
            stmt.executeUpdate( "CREATE TABLE " + str_table_name + "_Request ( Request varchar(4096),Counter int )");
            stmt.executeUpdate( "CREATE TABLE " + str_table_name + "_ReferrerHost ( ReferrerHost varchar(256),Counter int )");
            stmt.executeUpdate( "CREATE TABLE " + str_table_name + "_SearchKeyWords ( SearchKeyWords varchar(1024),Counter int )");
        }
        catch (Exception e) {
            System. out.print("MYSQL ERROR:" + e.getMessage() + "\n");
        }
        con_MySQL.close();
      
        Hdfstosql("/Logstash/Agent_Count_File/part-r-00000",(str_table_name+ "_Agent"),"Agent" );
        Hdfstosql("/Logstash/Agent_Count_File/part-r-00001",(str_table_name+ "_Agent"),"Agent" );
        Hdfstosql("/Logstash/Agent_Count_File/part-r-00002",(str_table_name+ "_Agent"),"Agent" );
        Hdfstosql("/Logstash/Agent_Count_File/part-r-00003",(str_table_name+ "_Agent"),"Agent" );
        Hdfstosql("/Logstash/Agent_Count_File/part-r-00004",(str_table_name+ "_Agent"),"Agent" );
        Hdfstosql("/Logstash/Agent_Count_File/part-r-00005",(str_table_name+ "_Agent"),"Agent" );
        Hdfstosql("/Logstash/IP_Count_File/part-r-00000",(str_table_name+ "_IP"),"IP" );
        Hdfstosql("/Logstash/IP_Count_File/part-r-00001",(str_table_name+ "_IP"),"IP" );
        Hdfstosql("/Logstash/IP_Count_File/part-r-00002",(str_table_name+ "_IP"),"IP" );
        Hdfstosql("/Logstash/IP_Count_File/part-r-00003",(str_table_name+ "_IP"),"IP" );
        Hdfstosql("/Logstash/IP_Count_File/part-r-00004",(str_table_name+ "_IP"),"IP" );
        Hdfstosql("/Logstash/IP_Count_File/part-r-00005",(str_table_name+ "_IP"),"IP" );
        Hdfstosql("/Logstash/Request_Count_File/part-r-00000",(str_table_name+ "_Request"),"Request" );
        Hdfstosql("/Logstash/Request_Count_File/part-r-00001",(str_table_name+ "_Request"),"Request" );
        Hdfstosql("/Logstash/Request_Count_File/part-r-00002",(str_table_name+ "_Request"),"Request" );
        Hdfstosql("/Logstash/Request_Count_File/part-r-00003",(str_table_name+ "_Request"),"Request" );
        Hdfstosql("/Logstash/Request_Count_File/part-r-00004",(str_table_name+ "_Request"),"Request" );
        Hdfstosql("/Logstash/Request_Count_File/part-r-00005",(str_table_name+ "_Request"),"Request" ); 
        Hdfstosql("/Logstash/ReferrerHost_Count_File/part-r-00000",(str_table_name+ "_ReferrerHost"),"ReferrerHost" );
        Hdfstosql("/Logstash/ReferrerHost_Count_File/part-r-00001",(str_table_name+ "_ReferrerHost"),"ReferrerHost" );
        Hdfstosql("/Logstash/ReferrerHost_Count_File/part-r-00002",(str_table_name+ "_ReferrerHost"),"ReferrerHost" );
        Hdfstosql("/Logstash/ReferrerHost_Count_File/part-r-00003",(str_table_name+ "_ReferrerHost"),"ReferrerHost" );
        Hdfstosql("/Logstash/ReferrerHost_Count_File/part-r-00004",(str_table_name+ "_ReferrerHost"),"ReferrerHost" );
        Hdfstosql("/Logstash/ReferrerHost_Count_File/part-r-00005",(str_table_name+ "_ReferrerHost"),"ReferrerHost" );
        Hdfstosql("/Logstash/KeyWords_Count_File/part-r-00000",(str_table_name+ "_SearchKeyWords"),"SearchKeyWords" );
        Hdfstosql("/Logstash/KeyWords_Count_File/part-r-00001",(str_table_name+ "_SearchKeyWords"),"SearchKeyWords" );
        Hdfstosql("/Logstash/KeyWords_Count_File/part-r-00002",(str_table_name+ "_SearchKeyWords"),"SearchKeyWords" );
        Hdfstosql("/Logstash/KeyWords_Count_File/part-r-00003",(str_table_name+ "_SearchKeyWords"),"SearchKeyWords" );
        Hdfstosql("/Logstash/KeyWords_Count_File/part-r-00004",(str_table_name+ "_SearchKeyWords"),"SearchKeyWords" );
        Hdfstosql("/Logstash/KeyWords_Count_File/part-r-00005",(str_table_name+ "_SearchKeyWords"),"SearchKeyWords" );  
       } 
}

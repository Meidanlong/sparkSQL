package meidl.utils.sql

object I020302Sql{

   def jkRwArrearsInter():StringBuffer = {
     val sql= new StringBuffer();
     sql.append("select ")
     sql.append("_c0 cust_ba, ")
     sql.append("_c1 bill_month, ")
     sql.append("_c2 amount ")
     sql.append("from jk_rw_arrears_inter")
     return sql;
   }


}
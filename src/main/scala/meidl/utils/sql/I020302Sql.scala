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

   def jkRwArrearsBusi():StringBuffer = {
     val sql= new StringBuffer();
     sql.append("SELECT a.*,b.BUSI_KIND_ID FROM jk_rw_arrears a,I020302_CA_UNTERMINAL b ")
     sql.append("where a.cust_ca_code = b.cust_ca_code ")
     sql.append("and a.cust_ba = b.cust_ba ")
     return sql;
   }

   def jkRwArrearsBusiJk():StringBuffer = {
     val sql= new StringBuffer();
     sql.append("SELECT c.channel_id,a.cust_ba,a.busi_kind_id,b.parent_chnl_kind_name,a.cust_ca_code,a.cust_name,a.bill_month,a.amount ")
     sql.append("FROM jk_rw_arrears_busi a,td_chl_kinddef b,jk_channel c ")
     sql.append("where a.busi_kind_id = b.chnl_kind_id ")
     sql.append("and b.parent_chnl_kind_id = c.PARENT_BUSI_KIND_ID ")
     sql.append("and a.only_code = c.only_code ")
     sql.append("and c.state = 1 ")
     return sql;
   }

   def insertJkRwArrears():StringBuffer = {
     val sql= new StringBuffer();
     sql.append("insert into jk_rw_arrears_meidl")
     sql.append("(id,channel_id,ba_code,cycle_id,arrears_time,amount,state,")
     sql.append("busi_kind_id,product_name,cust_ca_code,cust_name,bill_month) ")
     sql.append("values(?,?,?,?,?,?,?,?,?,?,?,?) ")
     return sql;
   }

  def main(args: Array[String]): Unit = {
    println(insertJkRwArrears)
  }
}
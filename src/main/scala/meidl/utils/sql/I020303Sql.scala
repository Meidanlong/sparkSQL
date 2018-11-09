package meidl.utils.sql

object I020303Sql{

  /**
    * " insert into jk_rw_data_unterminal \n"
    * + "   (qxtdiydx_l_id, only_code, organize_code, bi_channel_name, busi_valid_date, product_name, cust_ca_code,\n"
    * + "    cust_name, cust_ba, income, user_id, bill_month, create_time, ori_data_flag, bill_cycle,\n"
    * + "    touch_phone, mapping_boss, asset_update_date, accounts_status, product_status,\n"
    * + "    connect_product_status, connect_account_num, reward_type, client_status, status,\n"
    * + "    busi_exp_date,offer_id,offer_name)\n"
    * + " values"
    * + "    (jk_rw_data_busi_qx_info$seq.nextval, trim(?), ?, trim(?), to_date(?, 'yyyymmdd'), trim(?), trim(?), trim(?), ?, ?, ?, '"
    * + month
    * + "', sysdate, 1,\n"
    * + "'"
    * + month
    * + "', ?, ?, to_date(?, 'yyyy-mm-dd hh24:mi:ss'), ?, ?, ?, ?, trim(?), ?, 'Y',to_date(?, 'yyyymmdd'),?,?)";
    *
    * @return
    */
   def unterminalData():StringBuffer = {
     val sql= new StringBuffer();
     sql.append("select ")
     sql.append("_c0 only_code, ")
     sql.append("_c1 organize_code, ")
     sql.append("_c2 bi_channel_name, ")
     sql.append("_c3 cycle_id, ")
     sql.append("_c4 product_name, ")
     sql.append("_c5 cust_ca_code, ")
     sql.append("_c6 cust_name, ")
     sql.append("_c7 cust_ba, ")
     sql.append("_c8 busi_valid_date, ")
     sql.append("_c9  busi_exp_date, ")
     sql.append("_c10 product_status, ")
     sql.append("_c11 income, ")
     sql.append("_c12 touch_phone, ")
     sql.append("_c13 client_status, ")
     sql.append("_c14 mapping_boss, ")
     sql.append("_c15 accounts_status, ")
     sql.append("_c16 asset_update_date, ")
     sql.append("_c17 connect_product_status, ")
     sql.append("_c18 reward_type, ")
     sql.append("_c19 connect_account_num, ")
     sql.append("_c20 user_id, ")
     sql.append("_c21 offer_id, ")
     sql.append("_c22 offer_name ")
     sql.append("from unterminal_data ")
     return sql;
   }

  def main(args: Array[String]): Unit = {
    println(unterminalData().toString)
  }
}
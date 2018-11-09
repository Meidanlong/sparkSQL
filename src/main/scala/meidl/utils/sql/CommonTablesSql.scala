package meidl.utils.sql

object CommonTablesSql{

  def interfaceOrganizeCode():StringBuffer = {
    val sql= new StringBuffer;
    sql.append("select data_id,RSRV_STR3 from rwd_core_static t ")
    sql.append(" where t.type_id = 'INTERFACE_ORGANIZE_CODE' ")
    return sql;
  }

  def i020302Unterminal():StringBuffer = {
    val sql= new StringBuffer;
    sql.append("select * ")
    sql.append("from ")
    sql.append("i020302_unterminal t ")
    return sql;
  }

  def tdChlKinddef():StringBuffer = {
    val sql= new StringBuffer;
    sql.append("select * ")
    sql.append("from ")
    sql.append("td_chl_kinddef t ")
    return sql;
  }

  def jkChannel():StringBuffer = {
    val sql= new StringBuffer;
    sql.append("select * ")
    sql.append("from ")
    sql.append("jk_channel t ")
    return sql;
  }
}
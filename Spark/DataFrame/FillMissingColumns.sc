
/// Replacec missing DataFrame columns with null. Used to prepare DataFrame vertices for GraphFrame
def fillMissingColumns(myCols: Set[String], allCols: Set[String]) = {
  allCols.toList.map(x => x match {
    case x if myCols.contains(x) => col(x)
    case _ => lit(null).as(x)
  })
}


/*
Usage:
val users_v_cols = users_v.columns.toSet;
val posts_v_cols = posts_v.columns.toSet
val topics_v_cols = topics_v.columns.toSet
val tags_v_cols = tags_v.columns.toSet
val messages_v_cols = messages_v.columns.toSet
val users_reputation_v_cols = users_reputation_v.columns.toSet
val escrows_v_cols =escrows_v.columns.toSet
val payouts_v_cols = payouts_v.columns.toSet
val btcaddress_v_cols = btcaddress_v.columns.toSet
val payouts_btc_v_cols = payouts_btc_v.columns.toSet
val forums_v_cols = forums_v.columns.toSet
val all_cols = users_v_cols ++ posts_v_cols ++ topics_v_cols ++ tags_v_cols ++ messages_v_cols ++ users_reputation_v_cols ++ escrows_v_cols ++ payouts_v_cols ++ btcaddress_v_cols ++ payouts_btc_v_cols ++ forums_v_cols

val all_v = users_v.select(fillMissingColumns(users_v_cols, all_cols):_*)
.unionAll(posts_v.select(fillMissingColumns(posts_v_cols, all_cols):_*))
.unionAll(topics_v.select(fillMissingColumns(topics_v_cols, all_cols):_*))
.unionAll(tags_v.select(fillMissingColumns(tags_v_cols, all_cols):_*))
.unionAll(messages_v.select(fillMissingColumns(messages_v_cols, all_cols):_*))
.unionAll(users_reputation_v.select(fillMissingColumns(users_reputation_v_cols, all_cols):_*))
.unionAll(escrows_v.select(fillMissingColumns(escrows_v_cols, all_cols):_*))
.unionAll(payouts_v.select(fillMissingColumns(payouts_v_cols, all_cols):_*))
.unionAll(btcaddress_v.select(fillMissingColumns(btcaddress_v_cols, all_cols):_*))
.unionAll(payouts_btc_v.select(fillMissingColumns(payouts_btc_v_cols, all_cols):_*))
.unionAll(forums_v.select(fillMissingColumns(forums_v_cols,all_cols):_*))
*/
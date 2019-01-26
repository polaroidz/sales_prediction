package salespred.models

case class TrainingData(
    date: String,
    date_block_num: Int,
    shop_id: Int,
    item_id: Int,
    item_price: Double,
    item_cnt_day: Double
)
import org.apache.spark.sql.functions.{lit, udf}

case class Product(id: Int, code: String, name: String)
val products = Seq(Product(0, "P1", "Computer"), Product(1, "P2", "Phone"),
  Product(2, "P3", "Mirror")).toDS()
case class Region(id: Int, code: String, name: String)
val regions = Seq(Region(0, "L1", "HuaBei"), Region(1, "L2", "DongBei"),
  Region(2, "L3", "XiNan")).toDS()
case class Cost(id: Int, code: String, cost: Double)
val costs = Seq(Cost(0, "P1", 18), Cost(1, "P2", 10), Cost(2, "P3", 5)).toDS()
case class Sale(id: Int, product: String, region: String, month: Int,
  price: Double, quantity: Int)
val sales = Seq(Sale(1, "P1", "L1", 3, 60, 45),
  Sale(2, "P2", "L2", 3, 45, 80), Sale(3, "P2", "L3", 4, 45, 150),
  Sale(4, "P2", "L2", 5, 43, 80), Sale(5, "P2", "L2", 5, 46, 60),
  Sale(6, "P1", "L1", 6, 55, 30), Sale(7, "P3", "L2", 5, 23, 70)).toDS()

// split column
val temp11 = sales.select($"id".as[Int], $"product".as[String],
  $"region".as[String], $"month".as[Int])
val temp12 = sales.select($"id".as[Int], $"price".as[Double], $"quantity".as[Int])

// join column
val jd = temp11.joinWith(products, $"product" === $"code")
val jd1 = jd.map(s => (s._1._1, s._1._2, s._1._3, s._1._4, s._2.name))
val jd2 = jd1.joinWith(regions, $"_3" === $"code")
val jd3 = jd2.map(s => (s._1._1, s._1._2, s._1._3, s._1._4, s._1._5, s._2.name))
val jd4 = jd3.joinWith(costs, $"_2" === $"code")
val temp3 = jd4.map(s => (s._1._1, s._1._2, s._1._3, s._1._4, s._1._5, s._1._6,
  s._2.cost))
val jd5 = temp3.joinWith(temp12, $"_1" === $"id")
val temp5 = jd5.map(s => (s._1._1, s._1._2, s._1._3, s._1._4, s._1._5, s._1._6,
  s._1._7, s._2._2, s._2._3))

// filter
val temp6 = temp5.filter(_._5 == "Computer").filter(_._6 ==  "HuaBei")
val temp7 = temp6.withColumn("income", $"_8" * $"_9")
val temp9 = temp7.groupBy("_2", "_3", "_5", "_6").sum()
val res211 = temp9.withColumn("price", $"sum(income)" / $"sum(_9)")

val incomes = temp5.toDF().withColumn("income", $"_8" * $"_9")
val res212 = incomes.groupBy("_4", "_5", "_6").sum()

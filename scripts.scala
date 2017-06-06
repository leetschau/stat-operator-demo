case class Product(id: Int, code: String, name: String)
val products = Seq(Product(0, "P1", "Computer"), Product(1, "P2", "Phone"),
  Product(2, "P3", "Mirror")).toDF()
case class Region(id: Int, code: String, name: String)
val regions = Seq(Region(0, "L1", "HuaBei"), Region(1, "L2", "DongBei"),
  Region(2, "L3", "XiNan")).toDF()
case class Cost(id: Int, code: String, cost: Double)
val costs = Seq(Cost(0, "P1", 18), Cost(1, "P2", 10), Cost(2, "P3", 5)).toDF()
case class Sale(id: Int, product: String, region: String, month: Int,
  price: Double, quantity: Int)
val sales = Seq(Sale(1, "P1", "L1", 3, 60, 45),
  Sale(2, "P2", "L2", 3, 45, 80), Sale(3, "P2", "L3", 4, 45, 150),
  Sale(4, "P2", "L2", 5, 43, 80), Sale(5, "P2", "L2", 5, 46, 60),
  Sale(6, "P1", "L1", 6, 55, 30), Sale(7, "P3", "L2", 5, 23, 70)).toDF()

// split column
val df1 = sales.select($"id", $"product", $"region", $"month")
val df2 = sales.select($"id", $"price", $"quantity")

// join column
val addProduct = df1.join(products.select('code as "pcode", 'name as "pname"),
  'product === 'pcode).drop('pcode)
val addRegion = addProduct.join(regions.select('code as "rcode", 'name as "rname"),
  'region === 'rcode).drop('rcode)
val addCost = addRegion.join(costs.select('code as "pcode", 'cost),
  'product === 'pcode).drop('pcode)
val fullBase = addCost.join(df2, "id")

// filter
val compHuabei = fullBase.filter($"pname" === "Computer" && $"rname" === "HuaBei")
val addIncome = compHuabei.withColumn("income", $"price" * $"quantity")
val addPrice = addIncome.agg(sum("quantity"), sum("income")).
  withColumn("price", $"sum(income)" / $"sum(quantity)").
  withColumn("product", lit("Computer")).
  withColumn("region", lit("HuaBei"))

val totalIncome = fullBase.withColumn("income", $"price" * $"quantity")
val groupSum = totalIncome.groupBy("pname", "rname", "month").sum().
                 select('pname as "product", 'rname as "region", 'month,
                        $"sum(income)" as "income")

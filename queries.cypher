// 1) For each category the percentage of customers of each origin in descending order. E.g.: for the comb 30 percent Italian customers and 70 Argentinians
MATCH (c:Category)<-[:BELONGS_TO]-(:Product)<-[:CORRESPONDS_TO]-(:Item)<-[:COMPOSED_OF]-(:Order)<-[:PLACED]-(cu:Customer)
WITH c, toFloat(count(cu)) AS customer_number	//for each category how many customers
MATCH (c)<-[:BELONGS_TO]-(:Product)<-[:CORRESPONDS_TO]-(:Item)<-[:COMPOSED_OF]-(:Order)<-[:PLACED]-(cust:Customer),
		(cust)-[:LIVES_IN]->(:City)-[:PART_OF]->(st:State)
WITH c.name AS Category, st.code AS State, customer_number, round(100*count(cust)/customer_number, 2) AS Percentage //for each category the percentage of customers of each state
RETURN Category, State, Percentage 
ORDER BY Category ASC, Percentage DESC

// 2) For each company, the percentage of profit that each nation generates in decreasing order. For example, company X receives 70 percent from Italy and 30 percent from France
MATCH (s:Seller)<-[:SOLD_BY]-(i:Item)<-[:COMPOSED_OF]-(:Order)<-[:PLACED]-(cu:Customer),
		(cu)-[:LIVES_IN]->(:City)-[:PART_OF]->(:State)
WITH s, sum(i.price) AS revenue
MATCH (s:Seller)<-[:SOLD_BY]-(it:Item)<-[:COMPOSED_OF]-(:Order)<-[:PLACED]-(cust:Customer),
		(cust)-[:LIVES_IN]->(:City)-[:PART_OF]->(st:State)
WITH s.seller_id as Seller, st.code as State, revenue, round(100*sum(it.price)/revenue, 2) AS Percentage
RETURN Seller, State, Percentage
ORDER BY Seller ASC, Percentage DESC;

// 3) For each year the ranking of the top 10 profitable companies
MATCH (n:Order)
WITH DISTINCT n.purchase_timestamp.year AS Year
WITH Year, COLLECT{
    MATCH (s:Seller)<-[:SOLD_BY]-(i:Item)<-[:COMPOSED_OF]-(o:Order)
    WHERE o.purchase_timestamp.year = Year
    WITH s AS seller, round(sum(i.price), 2) AS Revenue
    RETURN {seller: seller.seller_id, revenue: Revenue}
    ORDER BY Revenue desc
    LIMIT 10
} as bs
UNWIND bs as BestSellers
RETURN Year, BestSellers.seller as Seller, BestSellers.revenue as Revenue
ORDER BY Year DESC, Revenue DESC;

// 4) For each company the percentage of profit coming from each product in descending order
MATCH (:Product)<-[:CORRESPONDS_TO]-(i:Item)-[:SOLD_BY]->(s:Seller)
WITH s, sum(i.price) AS revenue
MATCH (p:Product)<-[:CORRESPONDS_TO]-(it:Item)-[:SOLD_BY]->(s:Seller)
WITH s.seller_id as Seller, p.product_id as Product, revenue, round(100*sum(it.price)/revenue, 2) AS Profit_percentage
RETURN Seller, Product, Profit_percentage
ORDER BY Seller ASC, Profit_percentage DESC

// 5) For each category, the top ten products with the most positive average reviews
MATCH (cat:Category)
WITH cat, COLLECT{
    MATCH (cat)<-[:BELONGS_TO]-(p:Product)<-[:CORRESPONDS_TO]-(:Item)<-[:COMPOSED_OF]-(:Order)-[:RECEIVED]->(r:Review)
    WITH round(avg(r.score),2) as Average, p.product_id as Id
    RETURN {id: Id, score: Average}
    ORDER BY Average desc
    LIMIT 10
} as bs
UNWIND bs as BestSellers
RETURN cat.name as Category, BestSellers.id as Product, BestSellers.score as Score
ORDER BY Category ASC, Score DESC;
	
//6) For each seller the month when they earn the most
MATCH (o:Order)-[:COMPOSED_OF]->(i:Item)-[:CORRESPONDS_TO]->(p:Product),
		(i)-[:SOLD_BY]->(s:Seller)
WITH s.seller_id AS Seller, o.purchase_timestamp.month AS Month, round(sum(i.price), 2) AS Revenue
ORDER BY Revenue DESC
RETURN Seller, COLLECT(Month)[0] AS Month, COLLECT(Revenue)[0] AS Revenue
ORDER BY Seller ASC

// 7) For each customer, the average purchase frequency for each category

// VECCHIA

MATCH (cat:Category)<-[:BELONGS_TO]-(:Product)<-[:CORRESPONDS_TO]-(:Item)<-[:COMPOSED_OF]-(:Order)<-[:PLACED]-(cu:Customer)
WITH cu,cat,COUNT(cat) as for_each_category
MATCH (cate:Category)<-[:BELONGS_TO]-(:Product)<-[:CORRESPONDS_TO]-(:Item)<-[:COMPOSED_OF]-(:Order)<-[:PLACED]-(cust:Customer)
WITH cust,for_each_category, COUNT(cate) as total_category
MATCH (categ:Category)<-[:BELONGS_TO]-(:Product)<-[:CORRESPONDS_TO]-(:Item)<-[:COMPOSED_OF]-(:Order)<-[:PLACED]-(custo:Customer)
RETURN custo,categ,for_each_category,total_category, 100*for_each_category/total_category as percentage

// DA SISTEMARE (L'ho cambiata come For each category the average purchase frequency. Ho considerato due ordini effettuati lo stesso giorno come ordine unico. Ci sarebbe anche specificare che si tratta della frequenza degli ordini contenenti un item della categoria considerata; ordini contenti un numero diverso di item della stessa categoria sono stati trattati ugualmente. Se la vogliamo fare come fatta all'inizio basta cambiare solo la return)

MATCH (cat:Category)<-[:BELONGS_TO]-(:Product)<-[:CORRESPONDS_TO]-(Item)<-[:COMPOSED_OF]-(o:Order)<-[:PLACED]-(cu:Customer)
WITH cu.customer_id AS Customer, cat.name AS Category, datetime.truncate('day', o.purchase_timestamp) AS Date
WITH Customer, Category, COUNT(DISTINCT Date) as Count, toFloat(max(Date).epochSeconds-min(Date).epochSeconds) AS TimeSpan
WHERE Count>1
WITH Customer, Category, (365*24*3600*Count)/TimeSpan AS Frequency
RETURN Category, round(avg(Frequency), 2) AS Average_Frequency
ORDER BY Average_Frequency DESC
		
// 8) For each company, the percentages of collections made by credit card, boleto and other
MATCH (p:Payment)<-[:PAID_BY]-(o:Order)-[:COMPOSED_OF]->(:Item)-[:SOLD_BY]->(s:Seller)
WITH DISTINCT o.purchase_timestamp.year AS Year, s.seller_id AS Seller, SUM(p.value) AS Total
WITH Seller, Year, Total, COLLECT{
    MATCH (p:Payment)<-[:PAID_BY]-(o:Order)-[:COMPOSED_OF]->(:Item)-[:SOLD_BY]->(s:Seller)
    WHERE p.type = "credit_card" AND s.seller_id = Seller AND o.purchase_timestamp.year=Year
    WITH s, Total, 100*SUM(p.value)/Total as Credit_Card_Percentage
    RETURN Credit_Card_Percentage
}[0] AS Credit_Card, COLLECT{
    MATCH (p:Payment)<-[:PAID_BY]-(o:Order)-[:COMPOSED_OF]->(:Item)-[:SOLD_BY]->(s:Seller)
    WHERE p.type = "boleto" AND s.seller_id = Seller  AND o.purchase_timestamp.year=Year
    WITH s, Total, 100*SUM(p.value)/Total as Boleto_Percentage 
    RETURN Boleto_Percentage 
}[0] AS Boleto
RETURN Seller, Year, round(Credit_Card, 2) AS Credit_Card_Percentage, round(Boleto, 2) AS Boleto_Percentage, round(100-(Credit_Card+Boleto), 2) AS Other_percentage
ORDER BY Seller ASC, Year DESC

// 9) The average revenue generated by products grouped on the number of photos published

//MAGARI DA FARE SUL NUMERO DI PRODOTTI ORDINATI?
MATCH (i:Item)-[:CORRESPONDS_TO]->(pr:Product)
RETURN DISTINCT pr.photos_qty AS Photos_Quantity, round(avg(i.price),2) AS Revenue
ORDER BY Photos_Quantity DESC

// 10) For each year, the most purchased product
MATCH (o:Order)
WITH DISTINCT o.purchase_timestamp.year AS Year
WITH Year, COLLECT{
	MATCH (p:Product)<-[:CORRESPONDS_TO]-(:Item)<-[:COMPOSED_OF]-(o:Order)
	WHERE o.purchase_timestamp.year = Year
	WITH p AS product, COUNT(p) AS Quantity_Sold
	RETURN {product: product.product_id, quantity_sold: Quantity_Sold}
	ORDER BY Quantity_Sold DESC
	LIMIT 1
} AS bs
UNWIND bs AS Best_Sellers
RETURN Year, Best_Sellers.product AS Product, Best_Sellers.quantity_sold AS Quantity_Sold
ORDER by Year DESC

// 11) For each customer the last 10 products purchased
MATCH (c:Customer)
WITH DISTINCT c.customer_id AS Customer
WITH Customer, COLLECT{
	MATCH (c:Customer)-[:PLACED]->(o:Order)-[:COMPOSED_OF]->(:Item)-[:CORRESPONDS_TO]->(p:Product)
	WHERE c.customer_id = Customer
	WITH p AS product, o
	RETURN {product: product.product_id}
	ORDER BY o.purchase_timestamp DESC
	LIMIT 10
} AS lp
UNWIND lp AS Last_Products
RETURN Customer, Last_Products.product AS Product
ORDER BY Customer ASC


// 12) For each seller the last 10 products sold
MATCH (s:Seller)
WITH DISTINCT s.seller_id AS Seller
WITH Seller, COLLECT{
	MATCH (o:Order)-[:COMPOSED_OF]->(i:Item)-[:CORRESPONDS_TO]->(p:Product),
		(i)-[:SOLD_BY]->(s:Seller)
	WHERE s.seller_id = Seller
	WITH p AS product, o
	RETURN {product: product.product_id}
	ORDER BY o.purchase_timestamp DESC
	LIMIT 10
} AS lp
UNWIND lp AS Last_Products
RETURN Seller, Last_Products.product AS Product
ORDER BY Seller ASC

//NUOVA

MATCH (o:Order)-[:COMPOSED_OF]->(i:Item)-[:CORRESPONDS_TO]->(p:Product),
		(i)-[:SOLD_BY]->(s:Seller)
WITH s.seller_id AS Seller, p.product_id AS Products, o.purchase_timestamp AS TimeStamp
ORDER BY TimeStamp DESC
WITH Seller, COLLECT(Products)[0..10] AS Collection
UNWIND Collection AS Product
RETURN Seller, Product
ORDER BY Seller ASC

// 13) The average weight of items shipped to each city
MATCH (p:Product)<-[:CORRESPONDS_TO]-(:Item)<-[:COMPOSED_OF]-(:Order)<-[:PLACED]-(:Customer)-[:LIVES_IN]->(c:City)-[:PART_OF]->(s:State)
WITH c, s.code AS State, round(avg(p.weight_g),2) AS Target_Weight
RETURN c.name AS City, State, Target_Weight

// 14) For each customer the percentage of the state provenience of his/her orders
MATCH (c:Customer)-[:PLACED]->(:Order)-[:COMPOSED_OF]->(:Item)-[:SOLD_BY]->(se:Seller)
WITH c, count(se) AS Sellers_Number
MATCH (c)-[:PLACED]->(:Order)-[:COMPOSED_OF]->(:Item)-[:SOLD_BY]->(se:Seller),
		(se)-[:HAS_HEADQUARTERS_IN]->(:City)-[:PART_OF]->(st:State)
RETURN c.customer_id AS Customer, st.code AS State, Sellers_Number, round(100*toFloat(count(se))/Sellers_Number,2) AS Percentage
ORDER BY Customer ASC, Percentage DESC

// 15) For each seller the worst reviewed product
MATCH (se:Seller)
WITH DISTINCT se.seller_id AS Seller
WITH Seller, COLLECT{
	MATCH (s:Seller)<-[:SOLD_BY]-(:Item)-[:CORRESPONDS_TO]->(p:Product),
			(p)<-[:CORRESPONDS_TO]-(:Item)<-[:COMPOSED_OF]-(:Order)-[:RECEIVED]->(r:Review)
	WHERE s.seller_id = Seller
	WITH p AS product, round(avg(r.score), 2) AS Score
	RETURN {product: product.product_id, score: Score}
	ORDER BY Score ASC
	LIMIT 1
} AS wp
UNWIND wp AS Worst_Product
RETURN Seller, Worst_Product.product AS Product, Worst_Product.score AS Score

//CI IMPIEGA UN QUARTO DEL TEMPO QUESTA

MATCH (s:Seller)<-[:SOLD_BY]-(:Item)-[:CORRESPONDS_TO]->(p:Product),
			(p)<-[:CORRESPONDS_TO]-(:Item)<-[:COMPOSED_OF]-(:Order)-[:RECEIVED]->(r:Review)
WITH s.seller_id AS Seller, p.product_id AS Products, round(avg(toFloat(r.score)), 2) AS Average
ORDER BY Average ASC
RETURN Seller, COLLECT(Products)[0] AS Product, min(Average) AS Score
ORDER BY Seller ASC

// 16) For each seller the average size and weight of products sold
MATCH (p:Product)<-[:CORRESPONDS_TO]-(:Item)-[:SOLD_BY]->(s:Seller)
RETURN s.seller_id AS Seller, round(avg(p.width_cm),2) AS AVG_width_cm, round(avg(p.height_cm),2) AS AVG_height_cm, round(avg(p.length_cm),2) AS AVG_length_cm, round(avg(p.weight_g),2) AS AVG_weight

// 17) For each customer, the company from which he/she bought the most

//MAGARI DA CAMBIARE IN PER CIASCUN SELLER QUANTI CUSTOMER LA HANNO COME TOP? (BASTA CAMBIARE LA RETURN COME QUELLA COMMENTATA)
MATCH (c:Customer)
WITH DISTINCT c.customer_id AS Customer
WITH Customer, COLLECT{
	MATCH (c:Customer)-[:PLACED]->(o:Order)-[:COMPOSED_OF]->(:Item)-[:SOLD_BY]->(s:Seller)
	WHERE c.customer_id = Customer
	WITH s, COUNT(DISTINCT(o)) as Number_Orders
	RETURN {seller: s.seller_id, number_orders: Number_Orders}
	ORDER BY Number_Orders DESC
	LIMIT 1
} AS tp
UNWIND tp AS Top
RETURN Customer, Top.seller AS Seller, Top.number_orders AS Number_of_Orders
// RETURN Top.seller AS Seller, count(Customer) AS Number_Of_Customers

// 18) For each customer, the reviews that they wrote

MATCH (c:Customer)-[:PLACED]->(:Order)-[:RECEIVED]->(re:Review)
RETURN c,re

//LA CAMBIEREI IN For each customer, the average score of the reviews that they wrote, the count, and the category they reviewed the most:

MATCH (c:Customer)-[:PLACED]->(:Order)-[:RECEIVED]->(re:Review)
WITH c.customer_id AS Customer, round(avg(toFloat(re.score)), 2) as AVG_Score, count(re) AS Count
RETURN Customer, AVG_Score, Count, COLLECT{
	MATCH (c:Customer)-[:PLACED]->(o:Order)-[:RECEIVED]->(:Review)
	WHERE c.customer_id = Customer
	MATCH (cat:Category)<-[:BELONGS_TO]-(:Product)<-[:CORRESPONDS_TO]-(:Item)<-[:COMPOSED_OF]-(o:Order)
	WITH cat.name AS Category, count(o) AS Order_Count
	RETURN Category
	ORDER BY Order_Count DESC
	LIMIT 1
}[0] AS Most_Reviewed_Category
ORDER BY Count DESC


// 19) For each year, the best company based on the reviews score
MATCH (o:Order)
WITH DISTINCT o.purchase_timestamp.year AS Year
WITH Year, COLLECT{
	MATCH (o:Order)-[:RECEIVED]->(r:Review),
			(o)-[:COMPOSED_OF]->(:Item)-[:SOLD_BY]->(s:Seller)
	WHERE o.purchase_timestamp.year = Year
	WITH s, round(avg(toFloat(r.score)), 2) AS Score
	RETURN {seller: s.seller_id, score: Score}
	ORDER BY Score DESC
	LIMIT 1
} AS bs
UNWIND bs AS Best_Seller
RETURN Year, Best_Seller.seller AS Seller, Best_Seller.score AS Score
ORDER BY Year DESC

// 20) For each customer, how much he/she spent every year

MATCH (c:Customer)-[:PLACED]->(o:Order)-[:PAID_BY]->(p:Payment)
RETURN DISTINCT o.purchase_timestamp.year AS Year, c.customer_id AS Customer, sum(p.value) AS Total_Spending
ORDER BY Customer ASC, Year DESC

//Uguale, ma più complicata
MATCH (o:Order)
WITH DISTINCT o.purchase_timestamp.year AS Year
WITH Year, COLLECT{
	MATCH (c:Customer)-[:PLACED]->(o:Order)-[:PAID_BY]->(p:Payment)
	WHERE o.purchase_timestamp.year = Year
	WITH c,sum(p.value) AS Total_Spending
	RETURN {customer: c.customer_id, total_spending: Total_Spending}
} AS ts
UNWIND ts AS Total_Spending
RETURN Year, Total_Spending.customer AS Customer, Total_Spending.total_spending AS Total_Spending
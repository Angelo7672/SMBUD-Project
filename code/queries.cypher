// 1) For each category the percentage of customers of each origin in descending order. E.g.: for the comb 30 percent Italian customers and 70 Argentinians
MATCH (c:Category)<-[:BELONGS_TO]-(:Product)<-[:CORRESPONDS_TO]-(:Item)<-[:COMPOSED_OF]-(:Order)<-[:PLACED]-(cu:Customer)
WITH c, toFloat(count(cu)) AS customer_number	//for each category how many customers
MATCH (c)<-[:BELONGS_TO]-(:Product)<-[:CORRESPONDS_TO]-(:Item)<-[:COMPOSED_OF]-(:Order)<-[:PLACED]-(cust:Customer),
		(cust)-[:LIVES_IN]->(:City)-[:PART_OF]->(st:State)
WITH c.name AS Category, st.code AS State, customer_number, round(100*count(cust)/customer_number, 2) AS Percentage //for each category the percentage of customers of each state
RETURN Category, State, Percentage 
ORDER BY Category ASC, Percentage DESC;

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
MATCH (s:Seller)<-[:SOLD_BY]-(i:Item)<-[:COMPOSED_OF]-(o:Order)
WITH o.purchase_timestamp.year AS Year, s.seller_id AS Seller, round(sum(i.price), 2) AS Revenue
ORDER BY Revenue DESC
RETURN Year, COLLECT([Seller, Revenue])[0..10] AS Sellers
ORDER BY Year DESC;

// 4) For each company the percentage of profit coming from each product in descending order
MATCH (:Product)<-[:CORRESPONDS_TO]-(i:Item)-[:SOLD_BY]->(s:Seller)
WITH s, sum(i.price) AS revenue
MATCH (p:Product)<-[:CORRESPONDS_TO]-(it:Item)-[:SOLD_BY]->(s:Seller)
WITH s.seller_id as Seller, p.product_id as Product, revenue, round(100*sum(it.price)/revenue, 2) AS Profit_percentage
RETURN Seller, Product, Profit_percentage
ORDER BY Seller ASC, Profit_percentage DESC;

// 5) For each category, the top ten products with the most positive average reviews
MATCH (cat:Category)<-[:BELONGS_TO]-(p:Product)<-[:CORRESPONDS_TO]-(:Item)<-[:COMPOSED_OF]-(:Order)-[:RECEIVED]->(r:Review)
WITH cat.name AS Category, round(avg(r.score),2) as Average, p.product_id as Id
ORDER BY Average DESC
RETURN Category, COLLECT([Id, Average])[0..10] AS Products
ORDER BY Category ASC;
	
//6) For each seller the month when they earn the most
MATCH (o:Order)-[:COMPOSED_OF]->(i:Item)-[:CORRESPONDS_TO]->(p:Product),
		(i)-[:SOLD_BY]->(s:Seller)
WITH s.seller_id AS Seller, o.purchase_timestamp.month AS Month, round(sum(i.price), 2) AS Revenue
ORDER BY Revenue DESC
WITH Seller, COLLECT(Month)[0] AS Month, COLLECT(Revenue)[0] AS Revenue
RETURN Seller, ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"][Month-1] AS Month, Revenue
ORDER BY Seller ASC;

// 7) For each product, the average number of times it is bought per month
MATCH (o:Order)
WITH datetime.truncate('day', max(o.purchase_timestamp)) AS Now
MATCH (p:Product)<-[:CORRESPONDS_TO]-(Item)<-[:COMPOSED_OF]-(o:Order)
WITH p.product_id AS Product, Now, COUNT(o) as Count, toFloat(Now.epochSeconds+24*3600-datetime.truncate('day', min(o.purchase_timestamp)).epochSeconds) AS TimeSpan
RETURN Product, round((30*24*3600*Count)/TimeSpan, 2) AS Frequency
ORDER BY Frequency DESC;
		
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
ORDER BY Seller ASC, Year DESC;

// 9) The revenue, the number of sells and the average price of products based on their number of published photos
MATCH (i:Item)-[:CORRESPONDS_TO]->(pr:Product)
WITH DISTINCT pr.photos_qty AS Photos_Quantity, round(sum(i.price),2) AS Revenue, count(i) AS Sold_Products
RETURN Photos_Quantity, Revenue, Sold_Products, round(Revenue/Sold_Products,2) AS Average_Price
ORDER BY Photos_Quantity DESC;

// 10) For each year, the most purchased product
MATCH (p:Product)<-[:CORRESPONDS_TO]-(:Item)<-[:COMPOSED_OF]-(o:Order)
WITH DISTINCT o.purchase_timestamp.year AS Year, p.product_id AS Product, COUNT(p) AS Quantity_Sold
ORDER BY Quantity_Sold DESC
RETURN Year, COLLECT(Product)[0] AS Product, COLLECT(Quantity_Sold)[0] AS Quantity_Sold
ORDER BY Year DESC;

// 11) For each customer the last 10 products purchased
MATCH (c:Customer)-[:PLACED]->(o:Order)-[:COMPOSED_OF]->(:Item)-[:CORRESPONDS_TO]->(p:Product)
WITH DISTINCT c.customer_id AS Customer, p.product_id AS Product, o.purchase_timestamp AS TimeStamp
ORDER BY TimeStamp DESC
RETURN Customer, COLLECT(Product)[0..10] AS Products
ORDER BY Customer ASC;

// 12) For each seller the last 10 products sold
MATCH (o:Order)-[:COMPOSED_OF]->(i:Item)-[:CORRESPONDS_TO]->(p:Product),
		(i)-[:SOLD_BY]->(s:Seller)
WITH s.seller_id AS Seller, p.product_id AS Products, o.purchase_timestamp AS TimeStamp
ORDER BY TimeStamp DESC
WITH Seller, COLLECT(Products)[0..10] AS Collection
UNWIND Collection AS Product
RETURN Seller, Product
ORDER BY Seller ASC;

// 13) The average weight of items shipped to each city
MATCH (p:Product)<-[:CORRESPONDS_TO]-(:Item)<-[:COMPOSED_OF]-(:Order)<-[:PLACED]-(:Customer)-[:LIVES_IN]->(c:City)-[:PART_OF]->(s:State)
WITH c, s.code AS State, round(avg(p.weight_g),2) AS Target_Weight_g
RETURN c.name AS City, State, Target_Weight_g;

// 14) For each customer the percentage of the state provenience of his/her orders
MATCH (c:Customer)-[:PLACED]->(:Order)-[:COMPOSED_OF]->(:Item)-[:SOLD_BY]->(se:Seller)
WITH c, count(se) AS Sellers_Number
MATCH (c)-[:PLACED]->(:Order)-[:COMPOSED_OF]->(:Item)-[:SOLD_BY]->(se:Seller),
		(se)-[:HAS_HEADQUARTERS_IN]->(:City)-[:PART_OF]->(st:State)
RETURN c.customer_id AS Customer, st.code AS State, Sellers_Number, round(100*toFloat(count(se))/Sellers_Number,2) AS Percentage
ORDER BY Customer ASC, Percentage DESC;

// 15) For each seller the worst reviewed product
MATCH (s:Seller)<-[:SOLD_BY]-(:Item)-[:CORRESPONDS_TO]->(p:Product),
			(p)<-[:CORRESPONDS_TO]-(:Item)<-[:COMPOSED_OF]-(:Order)-[:RECEIVED]->(r:Review)
WITH s.seller_id AS Seller, p.product_id AS Products, round(avg(toFloat(r.score)), 2) AS Average
ORDER BY Average ASC
RETURN Seller, COLLECT(Products)[0] AS Product, min(Average) AS Score
ORDER BY Seller ASC;

// 16) For each seller the average size and weight of products sold
MATCH (p:Product)<-[:CORRESPONDS_TO]-(:Item)-[:SOLD_BY]->(s:Seller)
RETURN s.seller_id AS Seller, round(avg(p.width_cm),2) AS AVG_width_cm, round(avg(p.height_cm),2) AS AVG_height_cm, round(avg(p.length_cm),2) AS AVG_length_cm, round(avg(p.weight_g),2) AS AVG_weight_g;

// 17) For each customer, the company from which he/she bought the most
MATCH (c:Customer)-[:PLACED]->(o:Order)-[:COMPOSED_OF]->(:Item)-[:SOLD_BY]->(s:Seller)
WITH DISTINCT c.customer_id AS Customer, s.seller_id AS Seller, COUNT(DISTINCT(o)) as Number_Orders
ORDER BY Number_Orders DESC
RETURN Customer, COLLECT(Seller)[0] AS Seller, COLLECT(Number_Orders)[0] AS Number_Of_Orders
ORDER BY Customer ASC;

// 18) For each customer, the average score of the reviews that they wrote, the count, and the category they reviewed the most
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
ORDER BY Count DESC;

// 19) For each year, the best company based on the reviews score
MATCH (o:Order)-[:RECEIVED]->(r:Review),
			(o)-[:COMPOSED_OF]->(:Item)-[:SOLD_BY]->(s:Seller)
WITH DISTINCT o.purchase_timestamp.year AS Year, s.seller_id AS Seller, round(avg(toFloat(r.score)), 2) AS Score
ORDER BY Score DESC
RETURN Year, COLLECT(Seller)[0] AS Seller, COLLECT(Score)[0] AS Score
ORDER BY Year DESC;

// 20) For each customer, how much he/she spent every year
MATCH (c:Customer)-[:PLACED]->(o:Order)-[:PAID_BY]->(p:Payment)
RETURN DISTINCT o.purchase_timestamp.year AS Year, c.customer_id AS Customer, sum(p.value) AS Total_Spending
ORDER BY Customer ASC, Year DESC;
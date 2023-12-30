//For each category the percentage of customers of each origin in descending order. E.g.: for the comb 30 percent Italian customers and 70 Argentinians
MATCH (c:Category)<-[:BELONGS_TO]-(:Product)<-[:CORRESPONDS_TO]-(:Item)<-[:COMPOSED_OF]-(:Order)<-[:PLACED]-(cu:Customer),
		(cu)-[:LIVES_IN]->(:City)-[:PART_OF]->(s:State)
WITH c,s,count(cu) AS states_number	//for each category by how many different states
MATCH (cat:Category)<-[:BELONGS_TO]-(:Product)<-[:CORRESPONDS_TO]-(:Item)<-[:COMPOSED_OF]-(:Order)<-[:PLACED]-(cust:Customer),
		(cust)-[:LIVES_IN]->(:City)-[:PART_OF]->(st:State)
RETURN cat,st,states_number,100*states_number/count(cust) AS percentage	//for each category how many customer / for each category by how many different states

		//DA SISTEMARE

//For each company, the percentage of profit that each nation generates in decreasing order. For example, company X receives 50 percent from Italy and 50 percent from France
MATCH (:Seller)<-[:SOLD_BY]-(i:Item)<-[:COMPOSED_OF]-(:Order)<-[:PLACED]-(cu:Customer),
		(cu)-[:LIVES_IN]->(:City)-[:PART_OF]->(st:State)
WITH st, sum(toInteger(i.price)) AS revenue
MATCH (sell:Seller)<-[:SOLD_BY]-(it:Item)<-[:COMPOSED_OF]-(:Order)<-[:PLACED]-(cust:Customer),
		(cust)-[:LIVES_IN]->(:City)-[:PART_OF]->(st:State)
RETURN sell, st,revenue, 100*sum(toInteger(it.price))/revenue AS percentage
ORDER BY percentage desc

//For each year the ranking of the most profitable companies
MATCH (s:Seller)<-[:SOLD_BY]-(i:Item)<-[:COMPOSED_OF]-(o:Order)
WITH o.purchase_timestamp.year AS year, s AS seller, sum(toInteger(i.price)) AS revenue
RETURN year, seller, revenue
ORDER BY revenue desc

MATCH (s:Seller)<-[:SOLD_BY]-(i:Item)<-[:COMPOSED_OF]-(o:Order)
WITH o.purchase_timestamp.year AS year, s AS seller, sum(toInteger(i.price)) AS revenue
ORDER BY year, revenue DESC
WITH year, collect({seller: seller, revenue: revenue})[0] AS bestSeller
RETURN year, bestSeller.seller AS bestSeller, bestSeller.revenue AS revenue;

//For each company the percentage of profit coming from each product in descending order
MATCH (:Product)<-[:CORRESPONDS_TO]-(i:Item)-[:SOLD_BY]->(s:Seller)
WITH s,sum(toInteger(i.price)) AS seller_reward
MATCH (p:Product)<-[:CORRESPONDS_TO]-(it:Item)-[:SOLD_BY]->(se:Seller)
RETURN se,p,seller_reward,100*toInteger(it.price)/seller_reward AS profit_percentage
ORDER BY profit_percentage desc

//For each category, the top ten products with the most positive average reviews
MATCH (c:Category)<-[:BELONGS_TO]-(:Product)<-[:CORRESPONDS_TO]-(:Item)<-[:COMPOSED_OF]-(:Order)-[:RECEIVED]->(r:Review)
RETURN c,r,toInteger(r.score)

ORDER BY r.score desc
LIMIT 10
	//DA SISTEMARE
	
//For each company, the ranking of products that have been reordered on average several times by the same customer
MATCH (p:Product)<-[:CORRESPONDS_TO]-(:Item)<-[:COMPOSED_OF]-(:Order)<-[:PLACED]-(c:Customer),
		(p)<-[:CORRESPONDS_TO]-(:Item)-[:SOLD_BY]->(s:Seller)
RETURN s, c, COUNT(p) as how_many_times		//mi sa che cosi' conta i prodotti comprati dalla stessa azienda, non controlla che sia lo stesso
ORDER BY how_many_times desc

//For each customer, the average purchase frequency for each category
MATCH (cat:Category)<-[:BELONGS_TO]-(:Product)<-[:CORRESPONDS_TO]-(:Item)<-[:COMPOSED_OF]-(:Order)<-[:PLACED]-(cu:Customer)
WITH cu,cat,COUNT(cat) as for_each_category
MATCH (cate:Category)<-[:BELONGS_TO]-(:Product)<-[:CORRESPONDS_TO]-(:Item)<-[:COMPOSED_OF]-(:Order)<-[:PLACED]-(cust:Customer)
WITH cust,for_each_category, COUNT(cate) as total_category
MATCH (categ:Category)<-[:BELONGS_TO]-(:Product)<-[:CORRESPONDS_TO]-(:Item)<-[:COMPOSED_OF]-(:Order)<-[:PLACED]-(custo:Customer)
RETURN custo,categ,for_each_category,total_category, 100*for_each_category/total_category as percentage
		//DA CONTROLLARE
		
//For each company, the percentage of collections made by credit card and boleto
MATCH (p:Payment)<-[:PAID_BY]-(:Order)-[:COMPOSED_OF]->(:Item)-[:SOLD_BY]->(s:Seller)
WITH s,COUNT(p) as total_payment
MATCH (p:Payment)<-[:PAID_BY]-(:Order)-[:COMPOSED_OF]->(:Item)-[:SOLD_BY]->(s:Seller)
WHERE p.type = "credit_card" OR p.type = "boleto"
RETURN s,total_payment, 100*COUNT(p)/total_payment as percentage

//The revenue generated by each product category based on the number of photos published
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
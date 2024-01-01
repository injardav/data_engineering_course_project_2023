MATCH (a1:Dim_Authors)-[:AUTHORED_BY]->(p:Dim_Publication)<-[:AUTHORED_BY]-(a2:Dim_Authors)
WHERE a1.author_id <> a2.author_id
MERGE (a1)-[r:COLLABORATED_WITH]->(a2)
ON CREATE SET r.count = 1
ON MATCH SET r.count = r.count + 1

from neo4j import GraphDatabase


dataset_number = 2
datasets_folders = [
    "datasets/dataset0_50mb",
    "datasets/dataset1_100mb",
    "datasets/dataset2_200mb"
]


def import_data(uri, user, password):
    driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def execute_query(tx, query):
        tx.run(query)
    
    nodes_queries = [
        f"""
            LOAD CSV WITH HEADERS FROM 'file:///{datasets_folders[dataset_number]}/customers.csv' AS row
            CREATE (:Customer {{
                customer_id: row.CUSTOMER_ID,
                x_customer_id: toFloat(row.x_customer_id),
                y_customer_id: toFloat(row.y_customer_id),
                mean_amount: toFloat(row.mean_amount),
                std_amount: toFloat(row.std_amount),
                mean_nb_tx_per_day: toFloat(row.mean_nb_tx_per_day),
                available_terminals: row.available_terminals,
                nb_terminals: toInteger(row.nb_terminals)
            }});
        """,
        f"""
            LOAD CSV WITH HEADERS FROM 'file:///{datasets_folders[dataset_number]}/terminals.csv' AS row
            CREATE (:Terminal {{
                terminal_id: row.TERMINAL_ID,
                x_terminal_id: toFloat(row.x_terminal_id),
                y_terminal_id: toFloat(row.y_terminal_id)
            }});
        """,
        f"""
            CALL apoc.periodic.iterate(
                \"LOAD CSV WITH HEADERS FROM 'file:///{datasets_folders[dataset_number]}/transactions.csv' AS row RETURN row\",
                \"
                    CREATE (:Transaction {{
                        transaction_id: row.TRANSACTION_ID,
                        customer_id: row.CUSTOMER_ID,
                        terminal_id: row.TERMINAL_ID,
                        tx_datetime: row.TX_DATETIME,
                        tx_amount: toFloat(row.TX_AMOUNT),
                        tx_time_seconds: toInteger(row.TX_TIME_SECONDS),
                        tx_time_days: toInteger(row.TX_TIME_DAYS)
                    }})
                \",
                {{ batchSize: 10000, batchMode: "BATCH" }}
            );
        """
    ]

    relationships_queries = [
        f"""
            CALL apoc.periodic.iterate(
                \" MATCH (c:Customer) MATCH (tr:Transaction) WHERE c.customer_id = tr.customer_id RETURN c, tr \",
                \" MERGE (tr)-[:REQUESTED_BY]->(c) \",
                {{ batchSize: 10000, batchMode: "BATCH" }}
            );
        """,
        f"""
            CALL apoc.periodic.iterate(
                \" MATCH (t:Terminal) MATCH (tr:Transaction) WHERE t.terminal_id = tr.terminal_id RETURN t, tr \",
                \" MERGE (tr)-[:ON]->(t) \",
                {{ batchSize: 10000, batchMode: "BATCH" }}
            );
        """,
        f"""
            CALL apoc.periodic.iterate(
                \" MATCH (tr:Transaction)-[:REQUESTED_BY]->(c:Customer) MATCH (tr)-[:ON]->(t:Terminal) RETURN tr, c, t \",
                \" MERGE (c)-[:USED]->(t) \",
                {{ batchSize: 10000, batchMode: "BATCH" }}
            );
        """,
        f"""
            CALL apoc.periodic.iterate(
                \" MATCH (c1:Customer)-[:USED]->(t:Terminal)<-[:USED]-(c2:Customer) WHERE c1 <> c2 WITH c1, c2, COUNT(t) AS sharedTerminalCount RETURN c1, c2, sharedTerminalCount \",
                \" MERGE (c1)-[rel:SHARED_TERMINALS]-(c2) SET rel.count = sharedTerminalCount \",
                {{ batchSize: 10000, batchMode: "BATCH" }}
            );
        """
    ]
    
    with driver.session() as session:
        print("Inizio caricamento nodi")
        for query in nodes_queries:
            session.execute_write(execute_query, query)
            # session.run(query)

        print("Fine caricamento nodi, inizio caricamento relazioni")

        for query in relationships_queries:
            session.execute_write(execute_query, query)
            # session.run(query)
    
        print("Fine caricamento relazioni")

    driver.close()

if __name__ == "__main__":
    import_data("bolt://localhost:7687", "neo4j", "12345678")

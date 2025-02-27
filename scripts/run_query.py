import argparse
from neo4j import GraphDatabase
from query_d_extensions import add_fields
import time

queries = {
    "a": """
        MATCH (X:Customer)-[st:SHARED_TERMINALS]-(Y:Customer)
        WITH X, Y, st.count AS sharedTerminals
        WHERE 
            sharedTerminals >= 3 AND
            abs(Y.mean_amount - X.mean_amount) <= X.mean_amount * 0.1
        RETURN 
            X.customer_id AS Customer_X,
            Y.customer_id AS Customer_Y,
            X.mean_amount AS SpendingAmount_X,
            Y.mean_amount AS SpendingAmount_Y;
    """,
    "b": """
        WITH date() AS current_date
        WITH current_date.year AS current_year, current_date.month AS current_month
        WITH current_year, current_month, CASE 
                WHEN current_month = 1 THEN 12 
                ELSE current_month - 1 
            END AS previous_month,
            CASE 
                WHEN current_month = 1 THEN current_year - 1 
                ELSE current_year 
            END AS previous_year
        MATCH (t_prev:Transaction)-[:ON]->(term:Terminal)
        WHERE toInteger(SPLIT(t_prev.tx_datetime, "-")[1]) = previous_month
        AND toInteger(SPLIT(t_prev.tx_datetime, "-")[0]) = previous_year
        WITH term, AVG(t_prev.tx_amount) AS avg_prev_month_import, current_month, current_year
        WITH term, avg_prev_month_import, avg_prev_month_import * 1.2 AS threshold, current_month, current_year
        MATCH (t_curr:Transaction)-[:ON]->(term)
        WHERE toInteger(SPLIT(t_curr.tx_datetime, "-")[1]) = current_month
        AND toInteger(SPLIT(t_curr.tx_datetime, "-")[0]) = current_year
        AND t_curr.tx_amount > threshold
        SET t_curr.fraudulent = true
        RETURN t_curr
    """,
    "e": """
        MATCH (t:Transaction)
        WITH t.period_of_day AS period, COUNT(t) AS total_transactions, 
            AVG(CASE WHEN t.fraudulent = true THEN 1 ELSE 0 END) AS avg_fraudulent_transactions
        RETURN period, total_transactions, avg_fraudulent_transactions
        ORDER BY period
    """
}

def run_query(query_index, customer_id=None, num_levels=3):
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "12345678"))
    
    def execute_query(tx, query):        
        tx.run(query)

    with driver.session() as session:
        print("Running query", query_index)
        start_time = round(time.time() * 1000)
        
        if query_index == "a":
            execute_query(session, queries["a"])
        elif query_index == "b":
            execute_query(session, queries["b"])
        elif query_index == "c":
            if customer_id is None:
                raise ValueError("customer_id is required for query c")
            if num_levels < 2:
                raise ValueError("num_levels must be at least 2")
            
            c_query = f"MATCH (u1:Customer {{customer_id: {customer_id}}})"

            for i in range(1, num_levels):
                c_query += f" MATCH (u{i})-[:SHARES_TERMINAL]-(u{i+1}:Customer)"

            where_clause = f" WHERE"
            for i in range(1, num_levels + 1):
                for j in range(i+1, num_levels + 1):
                    where_clause += f" u{i}.customer_id <> u{j}.customer_id AND"
            where_clause = where_clause[:-4] + " RETURN DISTINCT u" + str(num_levels) + ".customer_id AS co_customer"

            c_query += where_clause
            print(c_query)

            execute_query(session, c_query)
        elif query_index == "d":
            add_fields(session)
        elif query_index == "e":
            execute_query(session, queries["e"])

        end_time = round(time.time() * 1000)
        print(f"Completed after {end_time - start_time} ms.")

    driver.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--query_index", type=str, required=True, help="Query index (a, b, c, e).")
    parser.add_argument("--customer_id", type=int, required=False, help="For query c - customer id.")
    parser.add_argument("--num_levels", type=int, required=False, help="For query c - number of CO_CUSTOMER levels (default 3).")
    args = parser.parse_args()
    run_query(args.query_index, args.customer_id, args.num_levels)

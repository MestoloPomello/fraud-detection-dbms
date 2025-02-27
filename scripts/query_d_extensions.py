import random

def add_fields(tx):
    query = f"""
        MATCH (t:Transaction{{transaction_id: $transaction_id}})
        WITH t, toInteger(substring(SPLIT(t.tx_datetime, " ")[1], 0, 2)) as intDateTime
        SET t.period_of_day = CASE
            WHEN intDateTime >= 6 AND 
                intDateTime < 12 THEN "morning"
            WHEN intDateTime >= 12 AND 
                intDateTime < 18 THEN "afternoon"
            WHEN intDateTime >= 18 AND 
                intDateTime < 24 THEN "evening"
            ELSE "night"
        END,
        t.product_kind = $product_kind,
        t.feeling_of_security = $feeling_of_security
    """

    i = 0
    for record in tx.run("MATCH (t:Transaction) WHERE t.product_kind IS NULL RETURN t.tx_datetime AS tx_datetime, t.transaction_id AS transaction_id LIMIT 100"):
        print("ITERAZIONE", i)
        print("record", record)
        period_of_day = get_period_of_day(record["tx_datetime"])
        print("period_of_day", period_of_day)
        product_kind = random.choice(["high tech", "food", "clothing", "consumable", "other"])
        print("product_kind", product_kind)
        feeling_of_security = random.randint(1, 5)
        print("feeling_of_security", feeling_of_security)
        tx.run(
            query,
            product_kind=product_kind,
            feeling_of_security=feeling_of_security,
            transaction_id=record["transaction_id"]
        )
        i += 1



def create_friends(tx):
    # We calculate the feeling_of_security average for customer and terminal
    query_avg_feeling = """
        MATCH (c:Customer)-[:REQUESTED_BY]-(t:Transaction)-[:ON]->(terminal:Terminal)
        WITH c, terminal, AVG(t.security_feeling) AS avg_feeling, COUNT(t) AS count
        WHERE count > 3
        RETURN c.customer_id AS customer_id, terminal.terminal_id AS terminal_id, avg_feeling
    """
    results = tx.run(query_avg_feeling)
    customers = list(results)

    # We create FRIENDS_WITH relationships for customers with similar feeling
    query_create_friends = """
        MATCH (c1:Customer), (c2:Customer)
        WHERE c1.customer_id = $customer1_id AND c2.customer_id = $customer2_id
        MERGE (c1)-[:FRIENDS_WITH {
            terminal_id: $terminal_id,
            avg_feeling_diff: $avg_feeling_diff
        }]->(c2)
    """

    for c1 in customers:
        for c2 in customers:
            if c1["customer_id"] != c2["customer_id"] and c1["terminal_id"] == c2["terminal_id"]:
                diff = abs(c1["avg_feeling"] - c2["avg_feeling"])
                if diff < 1:
                    tx.run(query_create_friends,
                            customer1_id=c1["customer_id"],
                            customer2_id=c2["customer_id"],
                            terminal_id=c1["terminal_id"],
                            avg_feeling_diff=diff)


def get_period_of_day(tx_datetime):
    hour = int(tx_datetime.split(" ")[1].split(":")[0])
    if 6 <= hour < 12:
        return "morning"
    elif 12 <= hour < 18:
        return "afternoon"
    elif 18 <= hour < 24:
        return "evening"
    else:
        return "night"
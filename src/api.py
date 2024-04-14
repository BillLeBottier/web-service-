from datetime import datetime
from faker import Faker
from sqlalchemy import create_engine, text
import random
from flask import Flask, jsonify

# Chaîne de connexion à la base de données
db_string = "postgresql://postgres:password@localhost:8080/postgres"

engine = create_engine(db_string)

fake = Faker()

create_user_table_sql = """
CREATE TABLE IF NOT EXISTS "user" (
    id SERIAL PRIMARY KEY,
    firstname VARCHAR(255),
    lastname VARCHAR(255),
    age INTEGER,
    email VARCHAR(255),
    job VARCHAR(255)
);

"""

create_apps_table_sql = """
CREATE TABLE IF NOT EXISTS "apps" (
   id SERIAL PRIMARY KEY,
   appname VARCHAR(255),
   username VARCHAR(255),
   lastconnection TIMESTAMP WITHOUT TIME ZONE,
   user_id INTEGER REFERENCES "user"(id)
);
"""

def populate_tables():
    # for loop to create 1 to 100 users
    # generate and run sql to create users
    apps = ["facebook", "Instagram", "Tiktok", "Twitter"]
    for _ in range(100):
        firstname = fake.first_name()
        lastname = fake.last_name()
        age = random.randrange(18, 50)
        email = fake.email()
        job = fake.job().replace("'", "")
        print(firstname, lastname, age, email, job)

        insert_user_query = f"""
            INSERT INTO "user" (firstname, lastname, age, email, job)
            Values ('{firstname}', '{lastname}', '{age}', '{email}', '{job}')
            RETURNING id
        """
        user_id = run_sql_with_results(insert_user_query).scalar()

        num_apps = random.randint(1, 5)
        for i in range(num_apps):
            username = fake.user_name()
            lastconnection = datetime.now()
            app_name = random.choice(apps)
            sql_insert_app = f"""
                INSERT INTO "apps" (appname, username, lastconnection, user_id)
                VALUES ('{app_name}','{username}', '{lastconnection}', '{user_id}')
            """
            run_sql(sql_insert_app)


app = Flask(__name__)

@app.route("/users")
def get_users():
    users = run_sql_with_results("SELECT * FROM \"user\"")
    data = []
    for row in users:
        user = {
            "id": row[0],
            "firstname": row[1],
            "lastname": row[2],
            "age": row[3],
            "email": row[4],
            "job": row[5]
        }
        data.append(user)
    return jsonify(data)
        
def run_sql(query: str):
    with engine.connect() as connection:
        trans = connection.begin()
        connection.execute(text(query))
        trans.commit()

def run_sql_with_results(query: str):
    with engine.connect() as connection:
        trans = connection.begin()
        result = connection.execute(text(query))
        trans.commit()
        return result

if __name__ == '__main__':
    run_sql(create_user_table_sql)        
    run_sql(create_apps_table_sql)
    app.run(host="0.0.0.0", port=8081)
    populate_tables()

import os
from database.db import db


def init_all_tables():
    cursor = db.cursor()
    cursor.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    for root, dirnames, filenames in os.walk("database/tables"):
        for filename in filenames:
            if filename.endswith(".sql"):
                    print(f"LOG: creating table {filename}")
                    cursor.execute(open(f"{root}/{filename}", "r").read())
                    db.commit()

    cursor.close()

import sqlite3
import csv

# -----------------------------------------
# Connect to the SQLite database
# -----------------------------------------
connection = sqlite3.connect("shipment_database.db")
cursor = connection.cursor()

print("Connected to database...")

# -----------------------------------------
# STEP 1: Load products (Spreadsheet 0)
# This file already matches the database structure
# -----------------------------------------
with open("data/spreadsheet_0.csv", newline="") as file:
    reader = csv.DictReader(file)

    for row in reader:
        product_id = row["id"]
        name = row["name"]
        price = row["price"]

        cursor.execute(
            "INSERT INTO product (id, name, price) VALUES (?, ?, ?)",
            (product_id, name, price)
        )

print("Products inserted successfully")


# -----------------------------------------
# STEP 2: Read shipment locations (Spreadsheet 2)
# We store them in a dictionary so we can
# look them up while processing shipments
# -----------------------------------------
shipment_locations = {}

with open("data/spreadsheet_2.csv", newline="") as file:
    reader = csv.DictReader(file)

    for row in reader:
        shipment_id = row["shipment_identifier"]
        origin = row["origin"]
        destination = row["destination"]

        shipment_locations[shipment_id] = (origin, destination)

print("Shipment locations loaded")


# -----------------------------------------
# STEP 3: Process shipment items (Spreadsheet 1)
# Each row = one product inside a shipment
# We combine this with location data
# -----------------------------------------
with open("data/spreadsheet_1.csv", newline="") as file:
    reader = csv.DictReader(file)

    for row in reader:
        shipment_id = row["shipment_identifier"]
        product = row["product"]
        quantity = row["quantity"]

        # get origin & destination from spreadsheet 2
        origin, destination = shipment_locations[shipment_id]

        # Insert shipment only once
        cursor.execute("""
            INSERT OR IGNORE INTO shipment (shipment_identifier, origin, destination)
            VALUES (?, ?, ?)
        """, (shipment_id, origin, destination))

        # Insert the product inside that shipment
        cursor.execute("""
            INSERT INTO shipment_product (shipment_identifier, product, quantity)
            VALUES (?, ?, ?)
        """, (shipment_id, product, quantity))

print("Shipments inserted successfully")


# -----------------------------------------
# Save changes and close connection
# -----------------------------------------
connection.commit()
connection.close()

print("Database population complete!")

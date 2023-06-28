import psycopg2 

# Connection details

host = "xxx"
database = "xxx"
user = "xxx"
password = "xxx"
port = "5432"

try:
    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(host=host,
                            port=port,
                            database=database,
                            user=user,
                            password=password)
    print("Connection Successfull")

except:
    print("Connection Error")

finally:

    print("Fetching Schemas...")

    #opens cursor
    cursor = conn.cursor()
    #executes sql
    cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT LIKE 'pg_%' AND schema_name != 'information_schema'")
    #collects results of query (all scheme names)
    schemas = cursor.fetchall()

    
    print("Generating SQL...")
    # SQL statements for creating the triggers
    trigger_function = "audit_trigger_function"
    insert_trigger = "CREATE TRIGGER audit_insert_trigger AFTER INSERT ON {table} FOR EACH ROW EXECUTE FUNCTION {function}()"
    update_trigger = "CREATE TRIGGER audit_update_trigger AFTER UPDATE ON {table} FOR EACH ROW EXECUTE FUNCTION {function}()"
    delete_trigger = "CREATE TRIGGER audit_delete_trigger AFTER DELETE ON {table} FOR EACH ROW EXECUTE FUNCTION {function}()"

    print("Looping Through Tables...")

    for schema in schemas:
        schema_name = schema[0]

        
        # Fetch all table names in the current schema
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s", (schema_name,))
        tables = cursor.fetchall()

        # Iterate over the tables and create triggers
        for table in tables:
            table_name = table[0]

            # Check if the table is a view
            cursor.execute("SELECT table_type FROM information_schema.tables WHERE table_schema = %s AND table_name = %s", (schema_name, table_name))
            table_type = cursor.fetchone()[0]

            if table_type != 'VIEW':
                print("Insterting triggers in " , table_name , "...")

                cursor.execute("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s)", (schema_name, table_name))
                table_exists = cursor.fetchone()[0]
                
                if table_exists:
                    # Create insert trigger
                    cursor.execute(insert_trigger.format(table=table_name, function=trigger_function))

                    # Create update trigger
                    cursor.execute(update_trigger.format(table=table_name, function=trigger_function))

                    # Create delete trigger
                    cursor.execute(delete_trigger.format(table=table_name, function=trigger_function))

        
# Commit the changes
print("Committing Changes...")
conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()

print("Trigger Insertion Completed")

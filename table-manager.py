# Databricks notebook source
dbutils.widgets.text("input_path", "default_value", "Enter text")
# Retrieve the widget value
path = dbutils.widgets.get("input_path")

# COMMAND ----------

dbutils.fs.ls(path)

# COMMAND ----------

def list_files_and_dirs_recursive(path):
    # List to store all file and directory paths
    all_items = []
    
    # List contents of the current path
    try:
        items = dbutils.fs.ls(path)
    except Exception as e:
        print(f"Error reading path {path}: {e}")
        return all_items

    for item in items:
        # Add current item (file or directory) to the list
        if item.isFile():
            if item.path.endswith(".sql"):
                all_items.append(item.path)

        # If the item is a directory, recurse into it
        if item.isDir():
            all_items += list_files_and_dirs_recursive(item.path)

    return all_items

# Example usage
all_sql_items = list_files_and_dirs_recursive(path)
print(all_sql_items)


# COMMAND ----------

import traceback

execution_log = []
for sql_file in all_sql_items:
    execution_log_dict ={}
    sql_file = sql_file.replace("dbfs:","")
    execution_log_dict["file_path"] = sql_file
    try:
        sql_content = open(sql_file, "r").read()
        sql_statements = sql_content.split(";") 
        for statement in sql_statements: 
            if statement.strip(): 
                spark.sql(statement) 
        execution_log_dict["status"] = "executed"
    except Exception as ex:
        raise ex
        execution_log_dict["status"] = "failed"
        execution_log_dict["error"] = traceback.format_exc()
    execution_log.append(execution_log_dict)

print(execution_log)

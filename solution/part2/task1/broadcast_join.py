from pyspark import SparkContext


def initialize_spark_context(app_name):
    # Initialize SparkContext
    return SparkContext(appName=app_name)


def load_datasets(sc, employees_path, departments_path):
    # Load the datasets
    employees = sc.textFile(employees_path)
    departments = sc.textFile(departments_path)
    return employees, departments


def build_hash_table(dataset):
    # Build a hash table from the dataset
    hash_table = dataset.map(lambda line: (line.split(",")[0], line)).collectAsMap()
    return hash_table


def map_join(line, employees_hash_table):
    # Map the join operation
    join_key = line.split(",")[0]  # Extract the join column from V (department_id)
    if employees_hash_table:
        # HR exists, probe HR with the join column
        matches = [employees_hash_table.get(join_key, None)]
        records = [(r, line) for r in matches if r is not None]
    else:
        # HR doesn't exist, add V to HLi
        records = [(join_key, line)]
    return records


def perform_broadcast_join(employees, departments):
    # Build the hash table for the employees dataset
    employees_hash_table = build_hash_table(employees)

    # Map the join operation for the departments dataset
    join_result = departments.flatMap(lambda line: map_join(line, employees_hash_table))

    return join_result


def collect_results(join_result):
    # Collect and return the results
    broadcast_join_results = join_result.collect()
    return broadcast_join_results


def stop_spark_context(sc):
    # Stop SparkContext
    sc.stop()


def perform_broadcast_join_job(app_name, employees_path, departments_path):
    # Perform the broadcast join job
    sc = initialize_spark_context(app_name)
    employees, departments = load_datasets(sc, employees_path, departments_path)
    join_result = perform_broadcast_join(employees, departments)
    broadcast_join_results = collect_results(join_result)
    stop_spark_context(sc)
    return broadcast_join_results


if __name__ == "__main__":
    app_name = "BroadcastJoin"

    # Set the file paths for employeesR.csv and departmentsR.csv
    employees_path = "hdfs:///user/maria_dev/files/employeesR.csv"
    departments_path = "hdfs:///user/maria_dev/files/departmentsR.csv"

    # Perform the broadcast join job
    broadcast_join_results = perform_broadcast_join_job(app_name, employees_path, departments_path)

    # Print the results
    print("Broadcast Join Results:")
    for result in broadcast_join_results:
        print(result)

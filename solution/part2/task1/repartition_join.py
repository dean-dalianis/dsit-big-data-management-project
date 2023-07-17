from pyspark import SparkContext


def initialize_spark_context(app_name):
    # Initialize SparkContext
    return SparkContext(appName=app_name)


def load_datasets(sc, employees_path, departments_path):
    # Load the datasets
    employees = sc.textFile(employees_path)
    departments = sc.textFile(departments_path)
    return employees, departments


def map_join(record):
    # Map the join operation
    line = record[1]
    join_key = line[2]  # Extract the join column from V
    tagged_record = ("R", line) if record[0] == "employees" else ("L", line)  # Add a tag of either R or L to V
    return (join_key, tagged_record)  # Emit (join key, tagged record)


def perform_repartition_join(employees, departments):
    # Perform the repartition join
    repartition_join_result = employees.map(lambda line: ("employees", line)).union(
        departments.map(lambda line: ("departments", line))).map(map_join)
    reduced_join_result = repartition_join_result.groupByKey().flatMapValues(
        lambda records: [(r, l) for r in records if r[0] == "R" for l in records if l[0] == "L"])
    return reduced_join_result


def collect_results(join_result):
    # Collect and return the results
    repartition_join_results = join_result.collect()
    return repartition_join_results


def stop_spark_context(sc):
    # Stop SparkContext
    sc.stop()


def perform_repartition_join_job(app_name, employees_path, departments_path):
    # Perform the repartition join job
    sc = initialize_spark_context(app_name)
    employees, departments = load_datasets(sc, employees_path, departments_path)
    join_result = perform_repartition_join(employees, departments)
    repartition_join_results = collect_results(join_result)
    stop_spark_context(sc)
    return repartition_join_results


if __name__ == "__main__":
    app_name = "RepartitionJoin"

    # Set the file paths for employeesR.csv and departmentsR.csv
    employees_path = "hdfs:///user/maria_dev/files/employeesR.csv"
    departments_path = "hdfs:///user/maria_dev/files/departmentsR.csv"

    # Perform the repartition join job
    repartition_join_results = perform_repartition_join_job(app_name, employees_path, departments_path)

    # Print the results
    print("Repartition Join Results:")
    for result in repartition_join_results:
        print(result[1][0], result[1][1])

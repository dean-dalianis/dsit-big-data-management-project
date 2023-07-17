from pyspark import SparkContext

FILES_DIR_PATH = 'hdfs://master:9000/home/user/files/'


def Map(K, V):
    # Extract the join key from V depending on whether V is an employee or a department
    join_key = V[2] if K == 'employees' else V[0]
    # Add a tag of either R or L to V depending on whether V is an employee or a department
    tagged_record = ('R', V) if K == 'employees' else ('L', V)
    # Return a tuple of the join key and the tagged record
    return join_key, tagged_record


def Reduce(LIST_V_prime):
    # Create separate lists for employee and department records
    BR = [t for t in LIST_V_prime if t[0] == 'R']  # list of employee records
    BL = [t for t in LIST_V_prime if t[0] == 'L']  # list of department records
    # Create new records by joining each employee with each department
    new_records = [(r, l) for r in BR for l in BL]
    return new_records


# Function to perform the join operation
def perform_join():
    spark_context = SparkContext(appName='RepartitionJoin')

    # Load employees and departments data and split each line into fields
    employees = spark_context.textFile(FILES_DIR_PATH + 'employeesR.csv').map(lambda line: line.split(','))
    departments = spark_context.textFile(FILES_DIR_PATH + 'departmentsR.csv').map(lambda line: line.split(','))

    # Apply the map function to employees and departments data
    map_employees = employees.map(lambda line: Map('employees', line))
    map_departments = departments.map(lambda line: Map('departments', line))

    # Combine the employees and departments data
    join_result = map_employees.union(map_departments)

    # Apply the reduce function to perform the join operation
    reduce_result = join_result.groupByKey().flatMap(lambda KV: Reduce(KV[1]))

    repartition_join_results = reduce_result.collect()

    spark_context.stop()

    return repartition_join_results


if __name__ == '__main__':
    join_results = perform_join()

    print('Repartition Join Results:')
    for result in join_results:
        employee_id, employee_name, department_id = result[0][1]
        department_name = result[1][1][1]
        print('Employee ID: {}, Employee Name: {}, Department ID: {}, Department Name: {}'.format(employee_id,
                                                                                                  employee_name,
                                                                                                  department_id,
                                                                                                  department_name))

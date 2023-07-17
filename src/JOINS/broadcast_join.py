from pyspark import SparkContext

FILES_DIR_PATH = 'hdfs://master:9000/home/user/files/'


def Init(sc, employees, departments):
    if employees.count() < departments.count():  # If R < L
        # Create a hash table of R and broadcast it to all nodes
        employees_local = employees.keyBy(lambda row: row[2]).collectAsMap()
        HR = sc.broadcast(employees_local)  # Broadcast the hash table
        HR_exists = True
        HL = None
    else:
        # Create a hash table of L and broadcast it to all nodes
        departments_local = departments.keyBy(lambda row: row[0]).collect()
        HR = None
        HR_exists = False
        HL = sc.broadcast(departments_local)  # Broadcast the hash table
    return HR, HL, HR_exists


def Map(line, HR, HR_exists):
    if HR_exists:
        # Probe the hash table (broadcasted) with the join column extracted from V
        join_key = line[0]
        matched_employee = HR.value.get(join_key)
        return [(matched_employee, line)] if matched_employee else []
    else:
        # Add V to an HLi partitioning it by its join column
        return [(line[0], line)]


def Close(line, HL):
    # Load Ri (partitions of R) in memory
    # For each record r in Ri do
    # Probe HLi with r’s join column
    join_key = line[2]
    matches = [x for x in HL.value if x[0] == join_key]
    # For each match l from HLi do
    return [(line, match[1]) for match in matches]


def perform_join():
    spark_context = SparkContext(appName='BroadcastJoin')

    # Load employees and departments data and split each line into fields
    employees = spark_context.textFile(FILES_DIR_PATH + 'employeesR.csv').map(lambda line: line.split(','))
    departments = spark_context.textFile(FILES_DIR_PATH + 'departmentsR.csv').map(lambda line: line.split(','))

    HR, HL, HR_exists = Init(spark_context, employees, departments)

    if HR_exists:
        # Perform broadcast join using the Map function
        join_results = departments.flatMap(lambda line: Map(line, HR, HR_exists))
    else:
        # Perform partitioned join using the Close function
        join_results = employees.flatMap(lambda line: Close(line, HL))

    join_results = join_results.collect()

    spark_context.stop()

    return join_results


if __name__ == '__main__':
    join_results = perform_join()

    print('Broadcast Join Results:')
    for result in join_results:
        employee, department = result
        employee_id, employee_name, department_id = employee
        department_id, department_name = department
        print('Employee ID: {}, Employee Name: {}, Department ID: {}, Department Name: {}'.format(employee_id,
                                                                                                  employee_name,
                                                                                                  department_id,
                                                                                                  department_name))

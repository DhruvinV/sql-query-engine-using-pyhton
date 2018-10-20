

from reading import *
from database import *




def run_query(database, query):
    '''(Database,str) -> Table
    This function runs the query on the given database and returns a
    Table resulting after proscessing the query.
    REQ: query must be in the form of valid SQuEaL Syntax
    '''
    # first split the query based on the blank spaces into parts to process
    # the query
    split_query = query.split(" ", 5)
    # create an empty list which will contain the names of tables
    # that will be selected from database to process the given query
    tables_to_select = []
    # create an empty list whcih will contain the names of columns from the
    # given tables that will be selected from table resulting after the
    # processing of the query
    columns_to_select = []
    # create an empty list called conditions which will contain all the
    # constraints that needs to be applied on table.
    conditions = []
    # use a indexed loop to go through the split_query list
    for i in range(0, len(split_query)):
        # check for from in the query and the string present in the index after
        # it will contain all the table names that needs to be selected
        if (split_query[i] == "from"):
            # split the string with "," as delimiter and the resultant list
            # will be the tables_to_select list.
            table = split_query[i + 1].split(",")
            tables_to_select = table
        # check for select in the query and the string present in the index
        # after it will contain all the column names that needs to be selected.
        elif (split_query[i] == "select"):
            # split the string with "," as delimiter and the resultant
            # list will be the columns_to_select list.
            table = split_query[i + 1].split(",")
            columns_to_select = table
        # check for select in the query and the string present in the index
        # after it will contain all the constraints that needs to be applied
        # in the query.
        elif (split_query[i] == "where"):
            # split the string with "," as delimiter and the resultant
            # list will be the columns_to_select list.
            table = split_query[i + 1].split(",")
            conditions = table
    # use select_tables function to select the tables from the database
    # that are given in the query. It will be list of object tables
    tables_in_query = select_tables(database, tables_to_select)
    # if there is only table there will be no cartesian product.
    if (len(tables_in_query) == 1):
        table = tables_in_query[0]
    # elif the table length is 2 calculate the cartesian product of table
    # present at 0th index of tables_of_database 1st index of
    # tables_of_database
    elif (len(tables_in_query) == 2):
        table = cartesian_product(tables_in_query[0], tables_in_query[1])
    # else calculate the cartesian_product of the table present at 0th index
    # of tables_of_database 1st index of tables_of_database
    else:
        # then calculate the cartesian product of the resultant table and use
        # loop to calculate the cartesian product of the table present at the
        # next index.
        table = cartesian_product(tables_in_query[0], tables_in_query[1])
        i = 2
        # run the loop until the length tables_in_query.
        while i < len(tables_in_query):
            table = cartesian_product(table, tables_in_query[i])
            # increase the counter
            i = i + 1
    # table_after_cartesian_product is the result of cartesian product of all
    # the tables_in_query.
    table_after_cartesian_product = table
    # use apply conditions functions on parameters
    # table_after_caartesian_product and condtions list to get a table
    # resulting after applying constraints provided in the conditions list.
    table_after_conditions = apply_conditions(
        table_after_cartesian_product, conditions)
    # final table will be the table resulting after selecting the columns
    # provided in the columns_to_select list. This will be done using select
    # columns function
    final_table = select_columns(table_after_conditions, columns_to_select)
    # the result will be final_table.
    result = final_table
    # return the result
    return result


def cartesian_product(table_1, table_2):
    '''(Table,Table) -> Table
    This function creates a new table where every single row from
    table_1 is matched with all the rows of Table_2 and returns the resultant
    table.
    '''
    # use get_columns method to get the columns of table_1 and table_2
    table_1_columns = table_1.get_columns()
    table_2_columns = table_2.get_columns()
    # use num_rows method to get the number of rows of table_1 and table_2
    no_of_rows_table1 = table_1.num_rows()
    no_of_rows_table2 = table_2.num_rows()
    # create a new_table 1 which is an empty table that will be used to fill
    # with columns of table resulting from the cartesian product table_1 and
    # table_2
    new_table1 = Table()
    # use get_columns method to get the columns of the table1
    table1_columns = new_table1.get_columns()
    # use for loop to go through every column element of table_1_column
    for i in table_1_columns:
        # create an empty list which will contain the column of table 1
        new_column_of_table1 = []
        # get the values of column1
        column1 = table_1_columns[i]
        # loop through each value and append it to new_column_of_table1
        # as many times as the length of of rows of table_2
        for k in column1:
            j = 0
            while j < no_of_rows_table2:
                new_column_of_table1.append(k)
                j = j + 1
        table1_columns[i] = new_column_of_table1
    # new_table 2 will contain the columns of cartesian product of table_2
    new_table2 = Table()
    table2_columns = new_table2.get_columns()
    # use for loop to go through every column of table_2_column
    for i in table_2_columns:
        # create an empty list which will contain the column of table
        new_column_of_table2 = []
        # get the values of column1
        column2 = table_2_columns[i]
        # loop through every column and append it to new_column_of_table2
        # as many time as the rows of of table 1
        j = 0
        while j < no_of_rows_table1:
            for k in column2:
                new_column_of_table2.append(k)
            j = j + 1
        table2_columns[i] = new_column_of_table2
    # create a new table called result_table ande add table1_columns and
    # table2_columns
    result_table = Table()
    result_table.add_column(table1_columns)
    result_table.add_column(table2_columns)
    # return the output
    return result_table


def conditions_for_equal_columns(table, conditions):
    '''(Table,str) -> Table
    This functions takes in table and process the conditions provided and
    return a proscessed table.
    REQ: conditions must contain the two column names that are present in the
    table and must have "=" joining the column names.
    '''
    # get the columns of the table using get_columns method of class table.
    columns = table.get_columns()
    # create an empty list called conditions_list which will contain the
    # conditions
    conditions_list = []
    # append the conditions to the conditions_list
    conditions_list.append(conditions)
    # split the conditions using "=" as delimiter
    column_names_in_conditions = conditions_list[0].split("=")
    # list of values0 will contain all the the values of column present at the
    # 0th index of column_names_in_conditions.
    list_of_values0 = columns[column_names_in_conditions[0]]
    # list of values1 will contain all the the values of column present at the
    # 1st index of column_names_in_conditions.
    list_of_values1 = columns[column_names_in_conditions[1]]
    # create a list an empty list called position_where_equal which would have
    # row positions where the list_of_value0 = list_of_value1
    position_where_equal = []
    # create a list of rows called same rows which satisifies the conditions.
    same_columns = []
    # loop through list_of_calues0 list to find the rows which satisfies the
    # conditions.
    for index in range(0, len(list_of_values0)):
        # if the list_of_values0 at ith index is equal to list_of_values1 at
        # ith index.
        if (list_of_values0[index] == list_of_values1[index]):
            # append the value to same_columns list
            same_columns.append(list_of_values0[index])
            # append the index number to position_where_equal.
            position_where_equal.append(index)
    # if the length of same_columns is 0 the no change will be made to existing
    # columns.
    if (len(same_columns) == 0):
        # final_columns will be the columns
        final_columns = columns
    # create a new_table.
    else:
        # create a new empty table that will be filled with columns fullfiling
        # the conditions.
        new_table = Table()
        # use get_columns method on new_table to the columns
        new_columns = new_table.get_columns()
        # instantiate new_columns of new_table and for column header present at
        # the 0th index of column_names_in_conditions and 1st index of
        # column_names_in_conditions to same_columns which is a list
        # fullfilling the conditions.
        new_columns[column_names_in_conditions[0]] = same_columns
        new_columns[column_names_in_conditions[1]] = same_columns
        # use a loop to go through rest of the columns in the table
        for colum_header in columns:
            i = colum_header
            # if the colum_header is not any one of the
            # column_names_in_conditions
            if ((i != column_names_in_conditions[0]) and (
                    i != column_names_in_conditions[1])):
                # create a list called new_column_values which will
                # contain all the values for rest of the column headers
                # satisfying the conditions
                new_column_values = []
                # use loop to go through position_where_equal which contains
                # all the positions where the condition is satisfied.
                for j in position_where_equal:
                    # use loop to go through all the index's of values of
                    # columns present in the table. if the index is equal to
                    # to any value in the list position_where_equal that means
                    # the conditions are satisfied.
                    for k in range(0, len(columns[i])):
                        if (k == j):
                            # if the condition is satisfied add that column
                            # value to the new_columns_value list
                            new_column_values.append(columns[i][k])
                # the colum_header in loop will new column values equal to new
                # _column_value list. and add this column to new_column_values.
                new_columns[i] = new_column_values
        # final_columns will be equal to all the columns present in the
        # new_column_values
        final_columns = new_columns
    # create an empty table called final_table which represents the table
    # obtained after processing the conditions
    final_table = Table()
    # add final_columns to final_table
    final_table.add_column(final_columns)
    # return the final_table
    return final_table


def conditions_for_greater_columns(table, conditions):
    '''(Table,str) -> Table
    This functions takes in table and process the conditions provided and
    return a proscessed table.
    REQ: conditions must contain the two column names that are present in the
    table and must have ">" joining the column names.
    '''
    # get the columns of the table using get_columns method of class table.
    columns = table.get_columns()
    # create an empty list called conditions_list which will contain the
    # conditions
    conditions_list = []
    # append the conditions to the conditions_list
    conditions_list.append(conditions)
    # split the string at 0th index of conditions_list using ">" as a delimiter
    column_names_in_conditions = conditions_list[0].split(">")
    # list of values0 will contain all the the values of column present at the
    # 0th index of column_names_in_conditions.
    list_of_values0 = columns[column_names_in_conditions[0]]
    # list of values0 will contain all the the values of column present at the
    # 1st index of column_names_in_conditions.
    list_of_values1 = columns[column_names_in_conditions[1]]
    # create a list an empty list called position_where_equal which would have
    # row positions where the list_of_value0 > list_of_value1
    position_where_true = []
    # create a empty list called same columns which will be filled with all the
    # columns satisifing the conditions.
    same_columns = []
    # loop through list_of_calues0 list to find the rows which satisfies the
    # conditions.
    for index in range(0, len(list_of_values0)):
        # if the list_of_values0 at ith index is greater than list_of_values1
        #  at ith index.
        if (float(list_of_values0[index]) > float(list_of_values1[index])):
            # append the value to same_columns list
            same_columns.append(list_of_values0[index])
            # append the index number to position_where_equal.
            position_where_true.append(index)
    # if the length of same_columns is 0 the no change will be made to existing
    # columns.
    if (len(same_columns) == 0):
        # final_columns will be the columns
        final_columns = columns
    else:
        # create a new empty table that will be filled with columns fullfiling
        # the conditions.
        new_table = Table()
        # use get_columns method on new_table to the columns
        new_columns = new_table.get_columns()
        # instantiate new_columns of new_table and for the column header
        # present at the 0th index of column_names_in_conditions to
        # same_columns which is list fullfilling the conditions
        new_columns[column_names_in_conditions[0]] = same_columns
        # use a loop to go through rest of the columns in the table
        for column_header in columns:
            i = column_header
            # if the colum_header is not the one of the
            # column_names_in_conditions present at the 0th index of the list.
            if (i != column_names_in_conditions[0]):
                # create a list called new_column_values which will
                # contain all the values for rest of the column headers
                # satisfying the conditions
                new_column_values = []
                # use loop to go through position_where_equal which contains
                # all the positions where the condition is satisfied.
                for j in position_where_true:
                    # use loop to go through all the index's of values of
                    # columns present in the table. if the index is equal to
                    # to any value in the list position_where_equal that means
                    # the conditions are satisfied.
                    for k in range(0, len(columns[i])):
                        # if the condition is satisfied add that column value
                        # to the new_columns_value list
                        if (k == j):
                            # if the condition is satisfied add that column
                            # value to the new_columns_value list
                            new_column_values.append(columns[i][k])
                # the colum_header in loop will have new column values equal to
                # new_column_value list. and add this column to new_columns
                new_columns[i] = new_column_values
        # final_columns will be equal to all the columns present in the
        # new_column_values
        final_columns = new_columns
        # create an empty table called final_table which represents the table
    # obtained after processing the conditions
    final_table = Table()
    # add final_columns to final_table
    final_table.add_column(final_columns)
    # # return the final_table
    return final_table


def conditions_for_greater_value(table, conditions):
    '''(Table,str) -> Table
    This functions takes in table and process the conditions provided and
    return a proscessed table.
    REQ: conditions must contain the one column name that is present in the
    table and must have ">" joining the column name and the value
    '''
    # get the columns of the table using get_columns method of class table
    columns = table.get_columns()
    # create an empty list called conditions_list which will contain the
    # conditions
    conditions_list = []
    # append the conditions to the conditions_list
    conditions_list.append(conditions)
    # split the conditions using ">" as delimiter
    names_in_conditions = conditions_list[0].split(">")
    # list of values0 will contain all the the values of column present at the
    # 0th index of names_in_conditions.
    list_of_values0 = columns[names_in_conditions[0]]
    # value is the that which is being compared with the column present at
    # the 0th index of names_in_conditions.
    value = names_in_conditions[1]
    # create a list an empty list called position_where_equal which would have
    # column positions where the list_of_value0 > value
    position_where_true = []
    # create a list of rows called same columns which satisifies the conditions
    same_columns = []
    # loop through list_of_calues0 list to find the column values
    # which satisfies the conditions.
    for index in range(0, len(list_of_values0)):
        # if the list_of_values0 at ith index is greater than value
        if (float(list_of_values0[index]) > float(value)):
            # append the value to same_columns list
            same_columns.append(list_of_values0[index])
            # append the index number to position_where_equal.
            position_where_true.append(index)
    # if the length of same_columns is 0 then no change will be made to
    # existing columns of the table.
    if (len(same_columns) == 0):
        # final_columns will be the columns
        final_columns = columns
    else:
        # create a new empty table that will be filled with columns fullfiling
        # the conditions.
        new_table = Table()
        # use get_columns method on new_table to the columns
        new_columns = new_table.get_columns()
        # instantiate new_columns of new_table and for column header present at
        # the 0th index of column_names_in_conditions to same_columns
        # is the list fullfilling the conditions.
        new_columns[names_in_conditions[0]] = same_columns
        # use a loop to go through rest of the columns in the table
        for column_header in columns:
            i = column_header
            # if the colum_header is not the one of the
            # column_names_in_conditions present at the 0th index of the list.
            if (i != names_in_conditions[0]):
                # create a list called new_column_values which will
                # contain all the values for rest of the column headers
                # satisfying the conditions
                new_column_values = []
                # use loop to go through position_where_equal which contains
                # all the positions where the condition is satisfied.
                for j in position_where_true:
                    # use loop to go through all the index's of values of
                    # columns present in the table. if the index is equal to
                    # to any value in the list position_where_equal that means
                    # the conditions are satisfied.
                    for k in range(0, len(columns[i])):
                        # if the condition is satisfied add that column value
                        # to the new_columns_value list
                        if (k == j):
                            new_column_values.append(columns[i][k])
                # the colum_header in loop will have new column values equal to
                # new_column_value list. and add this column to new_columns
                new_columns[i] = new_column_values
        # final_columns will be equal to all the columns present in the
        # new_columns
        final_columns = new_columns
        # create an empty table called final_table which represents the table
    # obtained after processing the conditions
    final_table = Table()
    # add final_columns to final_table
    final_table.add_column(final_columns)
    # return the final_table
    return final_table


def conditions_for_equal_value(table, conditions):
    '''(Table,str) -> Table
    This functions takes in table and process the conditions provided and
    return a proscessed table.
    REQ: conditions must contain the column name that is present in the
    table and must have "=" joining the olumn name and the value
    '''
    # get the columns of the table using get_columns method of class table
    columns = table.get_columns()
    # create an empty list called conditions_list which will contain the
    # conditions
    conditions_list = []
    # append the conditions to the conditions_list
    conditions_list.append(conditions)
    # split the conditions using "=" as delimiter
    names_in_conditions = conditions_list[0].split("=")
    # list of values0 will contain all the the values of column present at the
    # 0th index of names_in_conditions.
    list_of_values0 = columns[names_in_conditions[0]]
    # value is the that is which being compared with the column present at the
    # 0th index of names_in_conditions
    value = names_in_conditions[1]
    # create a list an empty list called position_where_equal which would have
    # column positions where the list_of_value0 = value
    position_where_equal = []
    # create a list of rows called same columns which will contain columns
    # values satisifies the conditions
    same_columns = []
    # loop through list_of_calues0 list to find the column values
    # which satisfies the conditions.
    for index in range(0, len(list_of_values0)):
        # if the list_of_values0 at ith index is equal to value
        if ((list_of_values0[index]) == (value)):
            # append the value to same_columns list
            same_columns.append(list_of_values0[index])
            # append the index number to position_where_equal.
            position_where_equal.append(index)
    # if the length of same_columns is 0 then no change will be made to
    # existing columns of the table.
    if (len(same_columns) == 0):
        final_columns = columns
    else:
        # create a new empty table that will be filled with columns fullfiling
        # the conditions.
        new_table = Table()
        # use get_columns method on new_table to the columns
        new_columns = new_table.get_columns()
        # instantiate new_columns of new_table and for set column header
        # present at the 0th index of names_in_conditions to
        # same_columns which is the list fullfilling the conditions.
        new_columns[names_in_conditions[0]] = same_columns
        # use a loop to go through rest of the columns in the table
        for column_header in columns:
            i = column_header
            # if the colum_header is not the one of the
            # names_in_conditions present at the 0th index of the list.
            if (i != names_in_conditions[0]):
                # create a list called new_column_values which will
                # contain all the values for rest of the column headers
                # satisfying the conditions
                new_column_values = []
                # use loop to go through position_where_equal which contains
                # all the positions where the condition is satisfied.
                for j in position_where_equal:
                    # use loop to go through all the index's of values of
                    # columns present in the table. if the index is equal to
                    # to any value in the list position_where_equal that means
                    # the conditions are satisfied.
                    for k in range(0, len(columns[i])):
                        # if the condition is satisfied add that column value
                        # to the new_columns_value list
                        if (k == j):
                            new_column_values.append(columns[i][k])
                # the colum_header in loop will have new column values equal to
                # new_column_value list. and add this column to new_columns
                new_columns[i] = new_column_values
                # the colum_header in loop will have new column values equal to
                # new_column_value list. and add this column to new_columns
        # final_columns will be equal to all the columns present in the
        # new_columns
        final_columns = new_columns
    # create an empty table called final_table which represents the table
    # obtained after processing the conditions
    final_table = Table()
    # add final_columns to final_table
    final_table.add_column(final_columns)
    # return the final table.
    return final_table


def select_columns(table, list_of_columns):
    '''(Table,list of str) -> Table
    The function takes an object table and list_of_columns and returns a object
    table with only the columns given in the list_of_columns.
    REQ: list_of_colummns should must correct squeal syntax for selecting
    columns from the table.
    '''
    # use the get_columns method of class table to get all the columns present
    # in the table.
    columns = table.get_columns()
    # create an empty Final table and get all the columns present in that table
    Final_table = Table()
    # Final_table_column will have no columns and based on the column names
    # add the columns from table to Final_table_columns
    Final_table_columns = Final_table.get_columns()
    # instatiate columns_to_select to list_of_columns
    columns_to_select = list_of_columns
    # if the list of columns has "*" which means select all columns from
    # table will be equal to Final_table_columns
    if (list_of_columns[0] == "*"):
        # Final_table_columns is columns
        Final_table_columns = columns
    # else select the columns from table to given in list_of_columns
    else:
        # use a loop to go through every column_headers in the columns
        for column_headers in columns:
            # if the column_headers match any of the values in the columns_to_
            # select
            for index in range(0, len(columns_to_select)):
                if (column_headers == columns_to_select[index]):
                    # if the above condition is satisfied then add that
                    # column_headers  and its values to Final_table_columns.
                    Final_table_columns[column_headers] \
                        = columns[column_headers]
    # add the Final_table_columns to Final_table.
    Final_table.add_column(Final_table_columns)
    # return the Final_table.
    return Final_table


def select_tables(database, list_of_tables):
    '''(Database,list of str) -> list of Table
    This function takes in object of type Database and list_of_tables
    corresponding to names of tables present in the database selectes those
    tables from the database and returns all the selected tables in list.
    REQ: All the names provided in the list_of_tables should correspond
    to a table in the database.
    '''
    # use tables method of database to get all the tables present in the
    # database.
    tables_in_database = database.tables()
    # create a tables_to_select which is equal to list_of_tables.
    tables_to_select = list_of_tables
    # create an empty list which will be filled with all the tables whose names
    # are present in the list of tables.
    result = []
    # use a element for loop to go through tables_in_database
    for tables in tables_in_database:
        # use a index loop to go through tables_to_select and compare all the
        # tables names in tables_of_select with tables.
        for index in range(0, len(tables_to_select)):
            # if tables is equal to any table name in tables_to_select
            if (tables == tables_to_select[index]):
                # append that table to result list.
                result.append(tables_in_database[tables])
    # return the result.
    return result


def apply_conditions(table, list_of_constraints):
    '''(Table,list of str) -> Table
    This funtions takes a table  object and valid list_of_constraints of
    squeal syntax and apply those to the table and returns the resultant table.
    REQ: list_of_constrains must have correct squeal query syntax.
    '''
    # if the list_of_costraints is empty table then there is no change to the
    # table and the result is same as the input table.
    if (list_of_constraints == []):
        result = table
        # else use the the input paramaters and call the appropriate functions
    # depending on the type of constraints provided in the query and return a
    # table.
    else:
        # use get_column_headers to get all the column_headers of columns
        # present in the table.
        table_column_headers = table.get_column_headers()
        # set result equal to table.
        result = table
        # use loop to go through all the conditions in list_of_constraints
        for conditions in list_of_constraints:
            # check for "=" in the conditions, if there is no = then it would
            # return -1 else.
            if (conditions.find("=") != -1):
                # split the str using as "=" as delimiter
                check_condition = conditions.split("=")
                # if the check_conditions[1] list has column_header that is
                # present in table_column_headers then
                if (check_condition[1] in table_column_headers):
                    # apply conditions_for_equal_columns on result and
                    # conditions
                    result = conditions_for_equal_columns(result, conditions)
                else:
                    # else apply conditions_for_equal_value for result and
                    # conditions.
                    result = conditions_for_equal_value(result, conditions)
            # check for ">" in the conditions, if there is no > then it would
            # return -1 else.
            elif (conditions.find(">") != -1):
                # split the str using as "=" as delimiter
                check_condition = conditions.split(">")
                # if the check_conditions[1] list has column_header that is
                # present in table_column_headers then
                if (check_condition[1] in table_column_headers):
                    # apply conditions_for_greater_columns on result and
                    # conditions
                    result = conditions_for_greater_columns(result, conditions)
                else:
                    # else apply conditions_for_equal_value for result and
                    # conditions.
                    result = conditions_for_greater_value(result, conditions)
    # return the result table.
    return result


if __name__ == "__main__":
    query = input("Enter a SQuEaL query, or a blank line to exit:")
    while(query != " "):
        database = read_database()
        result = run_query(database, query)
        result.print_csv()  


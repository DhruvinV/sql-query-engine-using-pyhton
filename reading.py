

# Functions for reading tables and databases

import glob
from database import *

file_list = glob.glob('*.csv')


def read_table(file_name):
        '''(str) -> Table
        This functions takes a file_name, reads  it and creates a object of
        type table and returns it.
        REQ: Files should with be CSV extension.
        '''
        # create a file_handle the file_name
        file_handle = file_name
        # open the file for reading to create each list where element is one
        # line of the of the file_name
        file_handle = open(file_name, 'r')
        # use readline method to read each line and strip the white spaces
        list_of_rows = file_handle.readline().strip("\n").strip()
        # create a table which is empty list that will be used to fill
        # each row of the table (each line of the file.)
        table = []
        # use a while loop until the line is empty.
        while list_of_rows != "":
                # append each line into the table list created above
                table.append(list_of_rows)
                # read the next line and remove and white spaces
                list_of_rows = file_handle.readline().strip("\n").strip()
        # close the file
        file_handle.close()
        # if the table list is empty then create a empty table called new_table
        if (table == []):
                new_table = Table()
        # else
        else:
                # create an empty table called new_table which will contain
                # empty columns which will be filled with the columns of
                # file that it contains.
                new_table = Table()
                new_column = new_table.get_columns()
                # Split the fist element of the table and create a list
                # list which contains all the column headers
                column_headers_of_table = table[0].split(",")
                # loop through each column headers in column_headers_of_table
                for column_headers in column_headers_of_table:
                        # find the index of each column headers in the list as
                        # that would be the index of the each row of of that
                        # column headers in the table list when each element
                        # is splited.
                        column_index = column_headers_of_table.index(
                            column_headers)
                        # create an empty list which would be filled with all
                        # the rows of the column_headers.
                        column_values = []
                        # set the counter to 1 and use while loop to go through
                        # rest of table list
                        i = 1
                        # use loop till the len of table that is the no of
                        # rows
                        while i < len(table):
                            # value of the column headers for the colum_headers
                            # would be every value present at the column_index
                            # in every element of list of table after splitting
                            # by ","
                                each_row_value = table[i].split(",")
                                # add the value to column_values
                                column_values.append(
                                    each_row_value[column_index].strip())
                                # increase the counter
                                i = i + 1
                        # column_headers contain all the column_values list.
                        new_column[column_headers] = column_values
                # add the new_column to new_table
                new_table.add_column(new_column)
        # return the new_table
        result = new_table
        # return the result
        return result


def read_database():
        '''() -> Database
        This function takes no parameters and creates a database of all the csv
        files present in the directory.
        '''
        # use glob.glob to get the file_list in the directory.
        file_list = glob.glob('*.csv')
        # create a table_names which contains all the table names of the
        # databaseas as a list
        table_names = []
        # create a list of tables which contains the table objects present in
        # directory.
        list_of_tables = []
        # loop through files in file_list.
        for files in (file_list):
                # the name_of_tables will be everything before "."
                name_of_tables = files.index(".")
                # add the name_of_tables to table_names.
                table_names.append(files[0:name_of_tables])
                # create a table object for each files
                list_of_tables.append(read_table(files))
        # create empty of database which will create empty tables
        new_database = Database()
        tables_of_database = new_database.tables()
        # loop through every index of table_names
        for tables in range(0, len(table_names)):
                # the table name(table_header) in the database will be
                # the item present at tables index
                table_header = table_names[tables]
                # and for that table name the table is the table object in
                # the list_of_table at tables index
                tables_of_database[table_header] = list_of_tables[tables]
        # add the table tables_of_database to new_database
        new_database.add_table(tables_of_database)
        # result will be the new_database
        result = new_database
        # return the result
        return result


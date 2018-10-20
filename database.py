

class Table():
    '''A class to represent a SQuEaL table'''
    def __init__(self, table_data=None):
        '''(Table,dict of {str: list of str}) -> NoneType
        this method Constructs the class table
        '''
        if table_data is None:
            self._table_data = {}
        else:
            self._table_data = table_data

    def set_dict(self, new_dict):
        '''(Table, dict of {str: list of str}) -> NoneType

        Populate this table with the data in new_dict.
        The input dictionary must be of the form:
            column_name: list_of_values
        '''
        self._table_data.update(new_dict)

    def get_dict(self):
        '''(Table) -> dict of {str: list of str}

        Return the dictionary representation of this table. The dictionary keys
        will be the column names, and the list will contain the values
        for that column.
        '''
        return self._table_data

    def add_column(self, new_dict):
        '''(Table,dict of {str: list of str}) -> Nonetype
        This method adds a column to this table.
        the input dicitionary must be of the form:
        column_name: list_of_values
        '''
        return self._table_data.update(new_dict)

    def get_columns(self):
        '''(Table) -> Nonetype
        This method gets all the column present in this table :
        '''
        return self._table_data

    def num_rows(self):
        '''(Table) -> int
        This method calculates the number of rows of the table object
        '''
        keys = []
        for i in self._table_data:
            keys.append(i)
        rows = []
        sample_key = keys[0]
        for i in (self._table_data[sample_key]):
            rows.append(i)
        result = len(rows)
        return result

    def remove_columns(self):
        '''(Table) -> empty dict
        This method removes all the columns from the table
        '''
        return self._table_data.clear()

    def get_column_headers(self):
        '''(Table) -> list of strings
        This method returns a column names present in the table as a list
        '''
        result = []
        for i in self._table_data:
            result.append(i)
        return result

    def print_csv(self):
        '''(Table) -> NoneType
        Print a representation of table in csv format.
        '''
        # no need to edit this one, but you may find it useful (you're welcome)
        dict_rep = self.get_dict()
        columns = list(dict_rep.keys())
        print(','.join(columns))
        rows = self.num_rows()
        for i in range(rows):
            cur_column = []
            for column in columns:
                cur_column.append(dict_rep[column][i])
            print(','.join(cur_column))


class Database():
    '''A class to represent a SQuEaL database'''
    def __init__(self, table=None):
        '''(Database,dict of{str:Table}) -> NoneType
        This method initialises the class Database
        '''
        if (table is None):
            self._database = {}
        else:
            self._database = table

    def set_dict(self, new_dict):
        '''(Database, dict of {str: Table}) -> NoneType

        Populate this database with the data in new_dict.
        new_dict must have the format:
            table_name: table
        '''
        self._database.update(new_dict)

    def get_dict(self):
        '''(Database) -> dict of {str: Table}

        Return the dictionary representation of this database.
        The database keys will be the name of the table, and the value
        with be the table itself.
        '''
        return self._database

    def add_table(self, new_dict):
        '''(Database, dict of {str: Table}) -> NoneType
        This method adds tables to the database
        '''
        return self._database.update(new_dict)

    def tables(self):
        '''(Database) -> dict of {str: Table}
        The method returns all the tables present in the database as a
        dicitionary.
        '''
        return self._database


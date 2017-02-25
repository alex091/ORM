from psycopg2 import extras, connect


class DatabaseError(Exception):
    pass


class NotFoundError(Exception):
    pass


class LockedForModification(Exception):
    pass


class Entity(object):
    db = None

    # ORM part 1
    __delete_query    = 'DELETE FROM "{table}" WHERE {table}_id=%s'
    __insert_query    = 'INSERT INTO "{table}" ({columns}) VALUES ({placeholders}) RETURNING "{table}_id"'
    __list_query      = 'SELECT * FROM "{table}"'
    __select_query    = 'SELECT * FROM "{table}" WHERE {table}_id=%s'
    __update_query    = 'UPDATE "{table}" SET {columns} WHERE {table}_id=%s'

    # ORM part 2
    __parent_query    = 'SELECT * FROM "{table}" WHERE {parent}_id=%s'
    __sibling_query   = 'SELECT * FROM "{sibling}" NATURAL JOIN "{join_table}" WHERE {table}_id=%s'
    __update_children = 'UPDATE "{table}" SET {parent}_id=%s WHERE {table}_id IN ({children})'
    __insert_junction_query = 'INSERT INTO "{table}" ({columns}) VALUES (%s, %s)'

    def __init__(self, id=None):
        if self.__class__.db is None:
            raise DatabaseError()

        self.__cursor   = self.__class__.db.cursor(
            cursor_factory=extras.DictCursor
        )
        self.__fields   = {}
        self.__id       = id
        self.__loaded   = False
        self.__modified = False
        self.__table    = self.__class__.__name__.lower()

    def __getattr__(self, name):
        # check, if instance is modified and throw an exception
        # get corresponding data from database if needed
        # check, if requested property name is in current class
        #    columns, parents, children or siblings and call corresponding
        #    getter with name as an argument
        # throw an exception, if attribute is unrecognized
        # if self.__modified is True:
        #     raise LockedForModification

        if name in self._columns:
            if not self.__loaded:
                self.__load()
            return self._get_column(name)
        elif name in self._parents:
            return self._get_parent(name)
        elif name in self._children:
            return self._get_children(name)
        elif name in self._siblings:
            return self._get_siblings(name)
        else:
            raise AttributeError

    def __setattr__(self, name, value):
        # check, if requested property name is in current class
        #    columns, parents, children or siblings and call corresponding
        #    setter with name and value as arguments or use default implementation
        if name in self._columns:
            self._set_column(name, value)
            self.__modified = True
        elif name in self._parents:
            self._set_parent(name, value)
            self.__modified = True
        elif name in self._siblings:
            self._set_sibling(name, value)
        else:
            super(Entity, self).__setattr__(name, value)

    def __execute_query(self, query, args=None):
        # execute an sql statement and handle exceptions together with transactions
        # print(query)
        # print(args)
        # print(self.__fields)
        try:
            if args is None:
                self.__cursor.execute(query)
            else:
                self.__cursor.execute(query, args)
            self.__class__.db.commit()
        except:
            self.__class__.db.rollback()
            raise DatabaseError

    def __insert(self):
        # generate an insert query string from fields keys and values and execute it
        # use prepared statements
        # save an insert id
        columns = []
        placeholders = []
        values = []

        for key, value in self.__fields.items():
            columns.append(key)
            placeholders.append('%s')
            values.append(value)

        columns_str = ', '.join(columns)
        placeholders_str = ', '.join(placeholders)

        self.__execute_query(self.__insert_query.format(table=self.__table, columns=columns_str,
                                                        placeholders=placeholders_str), values)
        self.__id = self.__cursor.fetchone()[0]

    def __load(self):
        # if current instance is not loaded yet — execute select statement and store it's result as an associative array (fields), where column names used as keys
        self.__execute_query(self.__select_query.format(table=self.__table), (self.__id,))
        self.__fields = dict(self.__cursor.fetchone())
        self.__loaded = True

    def __update(self):
        # generate an update query string from fields keys and values and execute it
        # use prepared statements
        columns = []
        values = []

        for key, value in self.__fields.items():
            columns.append("{} = %s".format(key))
            values.append(value)
        values.append(self.__id)
        columns = ",".join(columns)
        self.__execute_query(self.__update_query.format(table=self.__table, columns=columns), values)

    def _get_children(self,    name):
        # return an array of child entity instances
        # each child instance must have an id and be filled with data
        children_instances = []
        child_name = self._children[name]
        module = __import__('models')
        child_class = getattr(module, child_name)
        self.__execute_query(self.__parent_query.format(table=child_name.lower(), parent=self.__table), (self.__id,))
        query_result = self.__cursor.fetchall()

        for child in query_result:
            instance = child_class()
            instance._load_fields(child)
            instance.__id = instance.__fields['{}_id'.format(child_name.lower())]
            children_instances.append(instance)

        return children_instances


    def _get_column(self, name):
        # return value from fields array by <table>_<name> as a key

        return self.__fields["{}_{}".format(self.__table, name)]

    def _get_parent(self, name):
        # ORM part 2
        # get parent id from fields with <name>_id as a key
        # return an instance of parent entity class with an appropriate id
        parent_id = self.__fields['{}_id'.format(name)]
        module = __import__('models')
        inst = getattr(module, name.capitalize())

        return inst(parent_id)

    def _get_siblings(self, name):
        # ORM part 2
        # get parent id from fields with <name>_id as a key
        # return an array of sibling entity instances
        # each sibling instance must have an id and be filled with data

        sibling_instances = []
        #parent_id = self.__fields['{}_id'.format(name)]
        sibling_name = self._siblings[name]
        join_table = [sibling_name.lower(), str(self.__table)]
        join_table.sort()
        join_table = '__'.join(join_table)
        self.__execute_query(self.__sibling_query.format(sibling=sibling_name.lower(), join_table=join_table, table=self.__table), (self.__id,))
        query_result = self.__cursor.fetchall()
        module = __import__('models')
        sibling_class = getattr(module, sibling_name)

        for sibling in query_result:
            instance = sibling_class()
            instance._load_fields(sibling)
            instance.__id = instance.__fields['{}_id'.format(sibling_name.lower())]
            sibling_instances.append(instance)

        return sibling_instances


    def _set_column(self, name, value):
        # put new value into fields array with <table>_<name> as a key
        self.__fields['{}_{}'.format(self.__table, name)] = value

    def _set_parent(self, name, value):
        # ORM part 2
        # put new value into fields array with <name>_id as a key
        # value can be a number or an instance of Entity subclass
        if issubclass(type(value), Entity):
            self.__fields['{}_id'.format(name)] = value.id
        else:
            self.__fields['{}_id'.format(name)] = value

    def _set_sibling(self, name, value):
        junction = {self.__table: self.__id, self._siblings[name].lower(): value.__id}
        values = []
        columns = []
        tables = []
        for column, value in sorted(junction.items()):
            columns.append('{}_id'.format(column))
            values.append(value)
            tables.append(column)
        columns = ', '.join(columns)
        table = '__'.join(tables)
        self.__execute_query(self.__insert_junction_query.format(table=table, columns=columns), values)

    def _load_fields(self, dictionary):
        self.__fields = dict(dictionary)
        self.__loaded = True

    @classmethod
    def all(cls):
        # get ALL rows with ALL columns from corrensponding table
        # for each row create an instance of appropriate class
        # each instance must be filled with column data, a correct id and MUST NOT query a database for own fields any more
        # return an array of istances
        try:
            cursor = cls.db.cursor(
                cursor_factory=extras.DictCursor
                )
            table_name = str(cls.__name__.lower())
            cursor.execute(cls.__list_query.format(table=table_name))
            cls.db.commit()
            result = cursor.fetchall()
            instance_list = []

            for item in result:
                instance = cls()
                instance._load_fields(item)
                instance.__id = instance.__fields['{}_id'.format(table_name)]
                instance_list.append(instance)
            return instance_list
        except:
            cls.db.rollback()
            raise DatabaseError

    def delete(self):
        # execute delete query with appropriate id
        self.__execute_query(self.__delete_query.format(table=self.__table), (self.__id,))

    @property
    def id(self):
        # try to guess yourself
        return self.__id

    @property
    def created(self):
        return 'created: {}'.format(self.__fields['{}_created'.format(self.__table)])

    @property
    def updated(self):
        # try to guess yourself
        return 'updated: {}'.format(self.__fields['{}_updated'.format(self.__table)])

    def save(self):
        # execute either insert or update query, depending on instance id
        if self.__id is None:
            self.__insert()
        else:
            self.__update()
            self.__modified = False

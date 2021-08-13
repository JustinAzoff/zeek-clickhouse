#!/usr/bin/env python3
import sys

f = sys.argv[1]
d = open(f).read().splitlines()
fields = d[0].split()
types = d[1].split()
pairs = zip(fields, types)

def to_sql_type(type):
    if type.startswith(("set[", "vector[")):
        return "array"

    type_mapping = {
        'uid': 'string',
        'string': 'string',
        'addr': 'string',
        'enum': 'string',

        'time': 'number',
        'port': 'number',
        'count': 'number',
        'interval': 'number',
        'double': 'number',

        'bool': 'bool',
    }
    
    sql_type = type_mapping[type]
    return sql_type

def gen_field(field, type):
    sql_type = to_sql_type(type)

    return f"    `{sql_type}.values`[indexOf(`{sql_type}.names`, '{field}')] AS `{field}`,"


for f, t in pairs:
    if f.startswith("#"): continue
    print(gen_field(f, t))

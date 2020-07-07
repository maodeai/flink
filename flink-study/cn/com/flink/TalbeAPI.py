# -*- coding: utf-8 -*-
# @Time : 2020/7/7 17:35
# @Author : mh
# @Site : 
# @File : TalbeAPI.py
# @Software: PyCharm

from pyflink.dataset import ExecutionEnvironment
from pyflink.table import TableConfig, DataTypes, BatchTableEnvironment
from pyflink.table.descriptors import Schema, OldCsv, FileSystem

exec_env = ExecutionEnvironment.get_execution_environment()
exec_env.set_parallelism(3)
t_config = TableConfig()
t_evn = BatchTableEnvironment.create(exec_env, t_config)

t_evn.connect(FileSystem().path('/tmp/input')).with_format(OldCsv.field('word'),DataTypes.STRING()) \
    .with_schema(Schema().field('word'),DataTypes.STRING()) \
    .create_temporary_table('mySource')

t_evn.connect(FileSystem.path('/tmp/output')).with_format(OldCsv.field_delimiter('\t') \
                                                          .field('word',DataTypes.STRING() \
                                                           .with_schema()


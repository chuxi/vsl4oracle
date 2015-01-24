#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

"""

__author__ = 'king'

from fabric.api import *

env.user = 'vlis'
env.password = 'vlis@zju'

env.roledefs = {
    'test': ['10.214.20.118']
}

env.spark = 'node1'



@roles('test')
def dpconsumer():
    with settings(warn_only=True):
        result = put("./target/scala-2.10/KafkaMsgConsumer.jar", '/tmp/KafkaMsgConsumer.jar')
    if result.failed and not confirm("put jar file failed, Continue[Y/N]"):
        abort("Aborting file put jar task!")
    run('/usr/local/spark/bin/spark-submit --master spark://%s:7077  --class bigdata.KafkaMsgConsumer /tmp/KafkaMsgConsumer.jar %s vlis message 4 10.214.20.117 vsl epvsl' % (env.spark, env.roledefs['test'][0]))

@task
def deploy():
    execute(dpconsumer)
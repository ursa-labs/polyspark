import os, urllib.request, tarfile, json

def get_spark(spark_version, hadoop_version='2.7'):
    base = 'https://archive.apache.org/dist/spark/spark-%s/' % spark_version
    file = 'spark-%s-bin-hadoop%s' % (spark_version, hadoop_version)
    url = base + file + '.tgz'
    dirpath = '/tmp/polyspark/'
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)
    filepath = dirpath + file
    if not os.path.exists(filepath + '/bin'):
        urllib.request.urlretrieve(url, filepath + '.tgz')
        tar = tarfile.open(filepath + '.tgz', 'r:gz')
        tar.extractall(dirpath)
        tar.close()
    return filepath

def get_spark_submit(spark_version, hadoop_version='2.7'):
    return get_spark(spark_version, hadoop_version) + '/bin/spark-submit'

def run_on_spark(script_path, spark_version, hadoop_version='2.7', **kwargs):
    spark = get_spark_submit(spark_version, hadoop_version)
    cmd = spark + ' --master local ' + script_path
    if len(kwargs) > 0:
        cmd = cmd + ' "' + json.dumps(kwargs).replace('"', '\\"') + '"'
    os.system(cmd)

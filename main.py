import requests
import time

session_url = 'URL'
payload = {'kind': 'spark', 'conf': {
    "spark.master": "yarn-client",
    "spark.jars.packages": "com.datastax.spark:spark-cassandra-connector_2.10:1.6.2",
    "spark.driver.extraClassPath": "/home/livy/.ivy2/jars/com.datastax.spark_spark-cassandra-connector_2.10-1.6.2.jar:/home/livy/.ivy2/jars/joda-time_joda-time-2.3.jar:/home/livy/.ivy2/jars/com.twitter_jsr166e-1.1.0.jar:/home/livy/.ivy2/jars/io.netty_netty-all-4.0.33.Final.jar:/home/livy/.ivy2/jars/org.joda_joda-convert-1.2.jar:/home/livy/.ivy2/jars/org.scala-lang_scala-reflect-2.10.5.jar",
    "spark.executor.extraClassPath": "/home/livy/.ivy2/jars/com.datastax.spark_spark-cassandra-connector_2.10-1.6.2.jar:/home/livy/.ivy2/jars/joda-time_joda-time-2.3.jar:/home/livy/.ivy2/jars/com.twitter_jsr166e-1.1.0.jar:/home/livy/.ivy2/jars/io.netty_netty-all-4.0.33.Final.jar:/home/livy/.ivy2/jars/org.joda_joda-convert-1.2.jar:/home/livy/.ivy2/jars/org.scala-lang_scala-reflect-2.10.5.jar",
    "spark.cassandra.connection.host": "10.0.4.80,10.0.4.81,10.0.4.82",
    "spark.cassandra.auth.username": "USERNAME",
    "spark.cassandra.auth.password": "PW"
    }}
headers = {'content-type': 'application/json'}

def is_idle(session_url, id):
    t1 = time.clock()
    while True:
        time.sleep(1)
        r = requests.get(session_url + str(id))
        if(r.json()['state']=='idle'):
            return True
            print('leave loop')
        t2 = time.clock()
        if(t2-t1 > 10.0):
            return False

############################## MAIN ######################################
def main():

    r = requests.post(session_url, json=payload, headers=headers)
    id = r.json()['id']
    print(r.text)
    print(payload)

    print('start building session ' + str(id) )
    if(is_idle(session_url, id)):
        print('session ' + str(id) + ' build succeeded')
    else:
        print('session ' + str(id) + ' build failed')

#----------------------------- CODE -------------------------------
    code = '\
    import com.datastax.spark.connector._;\
    val cas_table = sc.cassandraTable("TABLE", "OBJECT")\
        .select("SELECT")\
        .map(row => row.toString().split(" ")(1))\
        .countByValue();\
    println(cas_table);'.replace("  ", "")

    print('try running code')
    r = requests.post(session_url + str(id) + '/statements', json={'code': code})
    statements_id = r.json()['id']
    print(r.text)
    # here I get the ID of a statement, thats counter for shared context
    # do we need shared RDDs ?

#----------------------------- RESULT -----------------------------
    if(is_idle(session_url, id)):
        result = requests.get(session_url + str(id) + '/statements')
        print('code execution succeeded')
        print(result.text)
    else:
        print('code execution failed')

    r = requests.delete(session_url + str(id))
    print(r.text)
    print('session ' + str(id) + ' was deleted')

if(__name__=="__main__"):
    main()

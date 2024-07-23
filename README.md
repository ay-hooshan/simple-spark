# simple-spark

## Run

> spark apps just runnable with spark-submit command. \
> don't run with the python3 command or anything else.

### Standalone way

- you should use from this way for debug and test!

1. download the spark including `spark-submit`.

2. go to your downloaded spark folder root.

3. start cluster master:

    ```shell
    ./sbin/start-master.sh 
    ```

4. start cluster workers:

    ```shell
    ./sbin/start-worker.sh <server-type>://<server-hostname>:<server-port>
    ```

    - example:

        ```shell
        ./sbin/start-worker.sh spark://abbas-ASUS:7077
        ```

5. run your code with `spark-submit` command:

    ```shell
    ./bin/spark-submit --master <master-address> --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 <your-code-address>
    ```

    - example:

        ```shell
        ./bin/spark-submit --master spark://abbas-ASUS:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /home/abbas/Documents/ay-repos/ay-hooshan/simple-spark/main.py
        ```

# Advanced-Database Systems NTUA


## Ομάδα 5:
 - Αλέξανδρος Τσάφος el18211
 <br>

## Installation
<br>

![Apache Spark](https://img.shields.io/static/v1?style=for-the-badge&message=Apache+Spark&color=E25A1C&logo=Apache+Spark&logoColor=FFFFFF&label=)
![Apache Hadoop](https://img.shields.io/static/v1?style=for-the-badge&message=Apache+Hadoop&color=222222&logo=Apache+Hadoop&logoColor=F1F100&label=)
![Scala](https://img.shields.io/static/v1?style=for-the-badge&message=Scala&color=DC322F&logo=Scala&logoColor=FFFFFF&label=)
![Shell](https://img.shields.io/static/v1?style=for-the-badge&message=Shell&color=222222&logo=Shell&logoColor=FFD500&label=)
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)

<br>

The project is implemented with `Apache Spark 3.1.3` over `Hadoop 2.7`. Both systems were installed in VM's provided by the Okeanos IaaS.

### Apache Spark Installation

Apache Spark was set-up by executing the following commands:

- Download and extract apache spark
  ```bash
  wget https://downloads.apache.org/spark/spark-3.1.3/spark-3.1.3-bin-hadoop2.7.tgz
  tar -xzf spark-3.1.3-bin-hadoop2.7.tgz
  ```

- Set Spark environment variables
  ```bash
  echo "SPARK_HOME=/home/user/spark-3.1.3-bin-hadoop2.7" >> ~/.bashrc
  echo 'PATH=$PATH:$SPARK_HOME/sbin' >> ~/.bashrc
  source ~/.bashrc
  ```

- Install correct java version
  ```bash
  sudo apt install openjdk-8-jdk
  java -version # Expected output: openjdk version "1.8.0_292"
  ```

- Set SPARK_MASTER_HOST
  ```bash
  touch ~/spark-3.1.3-bin-hadoop2.7/conf/spark-env.sh
  echo "SPARK_MASTER_HOST='192.168.0.1'" >> ~/spark-3.1.3-bin-hadoop2.7/conf/spark-env.sh
  ```
- Start Master Node (don't run in slave)
  ```bash
  start-master.sh
  ```


### HDFS Installation
Run the following commands in both master and worker node

- Setup passwordless SSH
  ```bash
  sudo apt-get install ssh
  ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
  cat .ssh/id_rsa.pub >> ~/.ssh/authorized_keys
  scp .ssh/authorized_keys 192.168.0.2:/home/ubuntu/.ssh/authorized_keys
  ```

- Add the machines in the known hosts 

  ```bash
  sudo vi /etc/hosts
  127.0.0.1       localhost
  192.168.0.1     snf-33140 
  192.168.0.2     snf-33141
  ```

- Download and extract Apache Hadoop
  ```bash
  wget https://archive.apache.org/dist/hadoop/common/hadoop-2.7.0/hadoop-2.7.0.tar.gz

  tar -xzf hadoop-2.7.0.tar.gz
  mv hadoop-2.7.0 hadoop
  ```

- Setup hadoop environmental variables
  ```bash
  echo 'HADOOP_HOME=/home/user/hadoop' >> ~/.bashrc
  echo 'PATH=$PATH:$HADOOP_HOME/bin' >> ~/.bashrc
  echo 'PATH=$PATH:$HADOOP_HOME/sbin' >> ~/.bashrc
  echo 'HADOOP_MAPRED_HOME=${HADOOP_HOME}' >> ~/.bashrc
  echo 'HADOOP_COMMON_HOME=${HADOOP_HOME}' >> ~/.bashrc
  echo 'HADOOP_HDFS_HOME=${HADOOP_HOME}' >> ~/.bashrc
  echo 'YARN_HOME=${HADOOP_HOME}' >> ~/.bashrc
  echo 'JAVA_HOME=$(readlink -f $(which java) | sed s:/jre/bin/java::g)' >> ~/.bashrc
  source ~/.bashrc
  ```
- Copy xml configuration files from repository folder to hadoop /etc folder

  ``` bash
  cp scripts/hdfs_config/* $HADOOP_HOME/etc/hadoop/
  ```

- Create HDFS Data folder
  ```bash
  sudo mkdir -p /usr/local/hadoop/hdfs/data
  sudo chown user:user -R /usr/local/hadoop/hdfs/data
  chmod 700 /usr/local/hadoop/hdfs/data
  ```
- Create masters/workers/slaves file
  ```bash
  masterIP = '192.168.0.1'
  slaveIP = '192.168.0.2'

  printf "$masterIP" > $HADOOP_HOME/etc/hadoop/masters
  printf "$masterIP\n$slaveIP" > $HADOOP_HOME/etc/hadoop/workers
  printf "$masterIP\n$slaveIP" > $HADOOP_HOME/etc/hadoop/slaves
  ```

 The following steps are to be executed **only for the master node**:


- Format HDFS and start it
  ```bash
  hdfs namenode -format
  start-yarn.sh
  start-dfs.sh
  ```

## Prepare the project
- Clone the repostitory in `~/ADB-code`
  ```bash
  git clone https://github.com/alextsaf/Advanced-Database-Systems-NTUA.git ~/ADB-code
  ```

- Download the Taxi Trips Data and Zone lookups
  ```bash
  /home/user/ADB-code/scripts/init.sh
  ```

- SBT builder and Scala we chosen for the project and have to be installed
  ```bash
  https://github.com/sbt/sbt/releases/download/v1.8.2/sbt-1.8.2.tgz
  tar -xzf sbt-1.8.2.tgz
  ```

## Run the app

To easily execute the app, the `run.sh` script has been created. Through this:
- One worker is started
- The app gets built by sbt and submitted in spark
- The results are moved to the results folder (`one-worker/`)
- A second is started
- The app gets built by sbt and submitted in spark
- The results are moved to the results folder (`two-workers/`)
- The workers stop
- `print-output.py` runs to 

  ```bash
  /home/user/ADB-code/scripts/run.sh
  ```



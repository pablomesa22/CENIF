#!/usr/bin/env bash
# Instala Apache Spark 3.5.3 en ~/spark (fuera del repositorio).
# Spark 3.5 necesita Java 8, 11 o 17. Si falla: brew install openjdk@17
#   y luego: export JAVA_HOME=$(/usr/libexec/java_home -v 17)
set -e
mkdir -p ~/spark && cd ~/spark
curl -O https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz
tar xzf spark-3.5.3-bin-hadoop3.tgz
export SPARK_HOME=~/spark/spark-3.5.3-bin-hadoop3
export PATH="$SPARK_HOME/bin:$PATH"
spark-shell --version
echo
echo "Para tenerlo siempre disponible, anade a ~/.zshrc:"
echo "  export SPARK_HOME=~/spark/spark-3.5.3-bin-hadoop3"
echo '  export PATH="$SPARK_HOME/bin:$PATH"'

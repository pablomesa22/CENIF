#!/usr/bin/env bash
# Instala Apache Spark 3.5.3 dentro de herramientas/spark (ignorado por git).
# Spark 3.5 necesita Java 8, 11 o 17. Si falla: brew install openjdk@17
#   y luego: export JAVA_HOME=$(/usr/libexec/java_home -v 17)
set -e
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
V=3.5.3
mkdir -p "$DIR/spark" && cd "$DIR/spark"
[ -f spark-$V-bin-hadoop3.tgz ] || curl -fLO https://archive.apache.org/dist/spark/spark-$V/spark-$V-bin-hadoop3.tgz
[ -d spark-$V-bin-hadoop3 ] || tar xzf spark-$V-bin-hadoop3.tgz
export SPARK_HOME="$DIR/spark/spark-$V-bin-hadoop3"
"$SPARK_HOME/bin/spark-submit" --version
echo
echo "SPARK_HOME=$SPARK_HOME"

#!/bin/bash

# Set the Cassandra host and port based on environment variables or use defaults
CASSANDRA_HOST=${CASSANDRA_SERVER_ADDRESS_ONE}
CASSANDRA_PORT=${CASSANDRA_SERVER_PORT}

echo "${CASSANDRA_SERVER_ADDRESS_ONE}"
echo "${CASSANDRA_SERVER_ADDRESS_TWO}"
echo "${CASSANDRA_SERVER_ADDRESS_THREE}"
echo "${CASSANDRA_SERVER_PORT}"

MAX_RETRIES=60
SLEEP_INTERVAL=10

retry=0

# Loop until Cassandra is accessible or max retries reached
while [ $retry -lt $MAX_RETRIES ]; do
#  nc -z $CASSANDRA_HOST $CASSANDRA_PORT && break
  ./cqlsh-astra/bin/cqlsh -e 'describe cluster' "${CASSANDRA_HOST}" "${CASSANDRA_PORT}" -u "${CASSANDRA_USER}" -p "${CASSANDRA_PASSWORD}"  && break
  retry=$((retry + 1))
  echo "Retrying ($retry/$MAX_RETRIES)..."
  sleep $SLEEP_INTERVAL
done

if [ $retry -eq $MAX_RETRIES ]; then
  echo "Timed out waiting for Cassandra to start."
  exit 1
fi

echo "Cassandra is up. Starting your application."

# Start your application here
# Example: java -jar your_application.jar
java -cp cassandraproject.jar org.cassandraproject.Main
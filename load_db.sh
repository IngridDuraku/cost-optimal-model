#!/bin/bash

HOST="127.0.0.1"
USER="<username>"
DATABASE="snowdb"
PASSWORD="<password>"
COMMAND_TEMPLATE="\\copy snowset from '<path_to_project>/cost-optimal-model/data/snowset_split/data_part_%d.csv' with (format csv, header true, delimiter ',');"

CONNECTION_URI="postgresql://$USER:$PASSWORD@$HOST/$DATABASE"

for ((i=3; i<=1730; i++)); do
    COMMAND=$(printf "$COMMAND_TEMPLATE" "$i")
    FULL_COMMAND="psql \"$CONNECTION_URI\" -c \"$COMMAND\""
    echo "Running command for data_part_$i.csv..."
    echo "Executing: $FULL_COMMAND"
    eval $FULL_COMMAND
    echo "Command for data_part_$i.csv executed."
done

#!/bin/bash

INIT_PORT=8000
echo $INIT_PORT
echo "number of slave: $1"

echo "add ports"

for i in $(seq 1 $1)
do
	PORT=$(($i + $INIT_PORT))
	
	echo "Listen $PORT" | sudo tee -a /etc/apache2/ports.conf > /dev/null

	echo -e "<VirtualHost *:$PORT>\n</VirtualHost>" | sudo tee -a /etc/apache2/sites-enabled/000-default.conf > /dev/null

	echo "successly open port $PORT"
	mkdir ./data$i
done

sudo service apache2 restart
echo "apache2 restart complete"

netstat -nplt

hostname -I
ant clean > /dev/null 2>&1
ant > /dev/null 2>&1
rm -rf data > /dev/null 2>&1
mkdir data > /dev/null 2>&1

java -jar m2-ecs.jar -p 40000 > /dev/null 2>&1 &
sleep 2

NUM_SERVERS=$1

BASE_PORT=50000


for (( i=0; i<NUM_SERVERS; i++ )); do
  PORT=$((BASE_PORT + i))
  
  java -jar m2-server.jar -b localhost:40000 -d ./data -p $PORT > /dev/null 2>&1 &
done
sleep 2

expect <<'END_EXPECT'
  log_user 0
  spawn java -jar m2-client.jar loglevel off
  expect "KVClient>"
  
  send "connect localhost 50000\r"
  expect "KVClient>"

  set start_time [clock seconds]


  for {set i 1} {$i <= 1000} {incr i} {
    send "put $i $i\r"
    expect "KVClient>"
  }

  set end_time [clock seconds]

  log_user 1
  
  set time_taken [expr {$end_time - $start_time}]
  puts "Time taken for puts: $time_taken seconds"

  log_user 0

  set get_start_time [clock seconds]

  for {set j 1} {$j <= 1000} {incr j} {
    send "get $j\r"
    expect "KVClient>"
  }

  set get_end_time [clock seconds]

  log_user 1

  set time_taken_get [expr {$get_end_time - $get_start_time}]
  puts "Time taken for gets: $time_taken_get seconds"

  log_user 0
  
  send "quit\r"
  
  expect eof
END_EXPECT

lsof -ti:40000 | xargs kill
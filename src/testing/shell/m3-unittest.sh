#!/bin/bash

ant clean > /dev/null 2>&1
ant > /dev/null 2>&1
rm -rf data > /dev/null 2>&1
mkdir data > /dev/null 2>&1

rm -rf logs > /dev/null 2>&1

BASE_PORT=50000
ECS_PORT=60000
PASS=0
FAIL=0

print_result(){
  if [ $1 -eq 0 ]; then
      echo "Passed"
      PASS=$((PASS + 1))
  else
      echo "Failed"
      echo "Error: $2" >&2
      FAIL=$((FAIL + 1))
  fi
}

# Test 1
# Launch one ECS
echo "Test 1: 1on1 "
echo -n "Launching one ECS... "

java -jar m3-ecs.jar -p $ECS_PORT > /dev/null 2>&1 &
ECS_PID=$!
sleep 5 # Wait for ECS to start
print_result $? "Failed to launch ECS"

# Launch one server
echo -n "Launching one server... "
PORT1=$((BASE_PORT + 1))
java -jar m3-server.jar -b localhost:$ECS_PORT -d ./data -p $PORT1 > /dev/null 2>&1 &
SPID1=$!
sleep 5 # Wait for servers to start
print_result $? "Failed to launch servers"

# Launch client
echo -n "Launching one client... "

commands=(
  "connect localhost $PORT1" 
  "put 1 a" "put 2 b"
  "put 3 c" "put 4 null"
  "get 1" "put 1 e"
  "get 2" "get 4 " 
  "get 5" "get 1"
)

for cmd in "${commands[@]}"; do
  echo "$cmd"
  sleep 5
done | java -jar m3-client.jar loglevel off > /dev/null 2>&1 &
print_result $? "Failed to launch clients"

# Test 2
echo "Test 2: 1on2 "
echo -n "Launching 2nd server... "
PORT2=$((BASE_PORT + 2))
java -jar m3-server.jar -b localhost:$ECS_PORT -d ./data -p $PORT2 > /dev/null 2>&1 &
SPID2=$!
sleep 5 # Wait for servers to start
print_result $? "Failed to launch servers"

# Launch client
echo -n "Launching one client... "

commands=(
  "connect localhost $PORT2"
  "get 1" "get 2" "get 4 " "get 5"
)

for cmd in "${commands[@]}"; do
  echo "$cmd"
  sleep 5
done | java -jar m3-client.jar loglevel off > /dev/null 2>&1 &
print_result $? "Failed to launch clients"

# Test 3
echo "Test 3: 1on3 "
echo -n "Launching 3rd server... "
PORT3=$((BASE_PORT + 3))
java -jar m3-server.jar -b localhost:$ECS_PORT -d ./data -p $PORT3 > /dev/null 2>&1 &
SPID3=$!
sleep 5 # Wait for servers to start
print_result $? "Failed to launch servers"

# Launch client
echo -n "Launching one client... "

commands=(
  "connect localhost $PORT3"
  "get 1" "get 2" "get 4 " "get 5"
)

for cmd in "${commands[@]}"; do
  echo "$cmd"
  sleep 5
done | java -jar m3-client.jar loglevel off > /dev/null 2>&1 &
print_result $? "Failed to launch clients"

# Test 4
echo "Test 4: 1on4 "
echo -n "Launching 4th server... "
PORT4=$((BASE_PORT + 4))
java -jar m3-server.jar -b localhost:$ECS_PORT -d ./data -p $PORT4 > /dev/null 2>&1 &
SPID4=$!
sleep 5 # Wait for servers to start
print_result $? "Failed to launch servers"

# Launch client
echo -n "Launching one client... "

commands=(
  "connect localhost $PORT4"
  "get 1" "get 2" "get 4 " "get 5"
)

for cmd in "${commands[@]}"; do
  echo "$cmd"
  sleep 5
done | java -jar m3-client.jar loglevel off > /dev/null 2>&1 &
print_result $? "Failed to operate client"

# Test 5
echo "Test 5: 1on5 "
echo -n "Launching 5th server... "
PORT5=$((BASE_PORT + 5))
java -jar m3-server.jar -b localhost:$ECS_PORT -d ./data -p $PORT5 > /dev/null 2>&1 &
SPID5=$!
sleep 5 # Wait for servers to start
print_result $? "Failed to launch servers"

# Launch client
echo -n "Launching one client... "

commands=(
  "connect localhost $PORT4"
  "put 5 is 4" "put 2 null"
  "get 1" "get 2" "get 4 " "get 5"
)

for cmd in "${commands[@]}"; do
  echo "$cmd"
  sleep 5
done | java -jar m3-client.jar loglevel off > /dev/null 2>&1 &
print_result $? "Failed to launch clients"

# Test 6
echo "Test 6: 1on6 "
echo -n "Launching 6th server... "
PORT6=$((BASE_PORT + 6))
java -jar m3-server.jar -b localhost:$ECS_PORT -d ./data -p $PORT6 > /dev/null 2>&1 &
SPID6=$!
sleep 5 # Wait for servers to start
print_result $? "Failed to launch servers"

# Launch client
echo -n "Launching one client... "

commands=(
  "connect localhost $PORT1"
  "put 6s q" "put g8 beihaidao"
  "get 6s" "get g8" 
  "get 5" "get 1" "get 2" "get 4"
)

for cmd in "${commands[@]}"; do
  echo "$cmd"
  sleep 5
done | java -jar m3-client.jar loglevel off > /dev/null 2>&1 &
print_result $? "Failed to launch client"

# Test 7
echo "Test 7: kill6 "
echo -n "Killing 6th server... "
kill -9 $SPID6
sleep 5

# Launch client
echo -n "Launching one client... "
commands=(
  "connect localhost $PORT3"
  "get 6s" "get g8" 
  "get 5" "get 1" 
  "get 2" "get 4"
)

for cmd in "${commands[@]}"; do
  echo "$cmd"
  sleep 5
done | java -jar m3-client.jar loglevel off > /dev/null 2>&1 &
print_result $? "Failed to launch clients"

# Test 8
echo "Test 8: kill 1 and 2 "
echo -n "Killing 1st server... "
kill -9 $SPID1
sleep 5
echo -n "Killing 2nd server... "
kill -9 $SPID2
sleep 5

# Launch client
echo -n "Launching one client... "
commands=(
  "connect localhost $PORT5"
  "get 6s" "get g8" 
  "get 5" "get 1" 
  "get 2" "get 4"
)

for cmd in "${commands[@]}"; do
  echo "$cmd"
  sleep 5
done | java -jar m3-client.jar loglevel off > /dev/null 2>&1 &
print_result $? "Failed to launch clients"

# Test 9
echo "Test 9: kill left servers"

echo "Killing 1st server... "
kill -9 $SPID3
sleep 5

echo "Killing 1st server... "
kill -9 $SPID5
sleep 5

# Launch client
echo -n "Launching one client... "
commands=(
  "connect localhost $PORT4"
  "get 6s" "get g8" 
  "get 5" "get 1" 
  "get 2" "get 4"
)

for cmd in "${commands[@]}"; do
  echo "$cmd"
  sleep 5
done | java -jar m3-client.jar loglevel off > /dev/null 2>&1 &
print_result $? "Failed to launch clients"

# Summarize test results
echo "Summary: $PASS tests passed, $FAIL tests failed"

lsof -ti:$ECS_PORT | xargs kill
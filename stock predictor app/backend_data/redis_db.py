import redis
r = redis.Redis(host='localhost', port=6379, db=0)

# Set a key
r.set('mykey', 'Hello World from Redis!')

# Get the key
value = r.get('mykey')
print(value.decode())  # Output: Hello from Redis!

# Build
`mvn package`

# Run Lettuce failover test

` java -jar target/redconn-0.0.1-SNAPSHOT.jar --driver=lettuce --host=redis-12000.internal.k1.demo.reslabs.com  --port=12000   --connectionTimeout=1 --socketTimeout=2`

# How to fail RL node
```
#!/bin/bash
iptables -t filter -A INPUT -p tcp -m tcp --dport 22 -j ACCEPT
iptables -t filter -A INPUT -j REJECT
iptables -t filter -A OUTPUT -p tcp -m tcp --sport 22 -j ACCEPT
iptables -t filter -A OUTPUT -j REJECT

echo Killed, waiting 60 seconds...
sleep 60

echo Resuming...

iptables -F INPUT
iptables -F OUTPUT

echo Done.
```


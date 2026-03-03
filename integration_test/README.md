## integration_test quick start

### Run

```bash
go run ./integration_test \
  -u 100 \
  -lgr 1 \
  -sm 0 \
  -gm 60 \
  -sem=true \
  -tgid 794058029 \
  -gsr 100
```

### Common flags

- `-u`: total simulated users
- `-lgr`: login rate (`1` means all users login first)
- `-sm`: single-chat message count per sender
- `-gm`: group message count per sender
- `-sem`: whether to send messages
- `-tgid`: only send group messages to this group ID
- `-gsr`: group send rate (messages/second, global across all senders)

### Notes

- With `-u 100 -lgr 1 -tgid <groupID> -gsr 100`, total group throughput targets about `100 msg/s`.
- Total messages in this mode are approximately `u * gm`.
- If any sender has not joined `-tgid`, the test fails fast for that user.

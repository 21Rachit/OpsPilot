# DNS latency high

Use this runbook when DNS lookups are slow. Users see slow page loads or login timeouts,
often in one region, while the application servers themselves look healthy.

## Symptoms

- Pages and logins are slow, but CPU and memory on the app servers are normal
- `dig` takes hundreds of milliseconds instead of a few
- Alerts such as "DNS latency high" or "resolver p99 above 200ms"

## Diagnose

1. Measure the lookup time from an affected host:

   ```bash
   dig api.example.com | grep "Query time"
   ```

2. Ask the secondary resolver directly and compare:

   ```bash
   # 10.0.2.53 is the secondary resolver in every region
   dig @10.0.2.53 api.example.com | grep "Query time"
   ```

3. Check CPU and query rate on the primary resolver dashboard. A resolver above 80% CPU
   answers slowly for everyone in its region.

## Fix

- If only the primary resolver is slow, fail over to the secondary: set it as the first
  nameserver in the region's DHCP options, then restart the resolver service on the primary.
- If both resolvers are slow, the upstream provider is the likely cause. Check its status page.

## Escalate

If latency stays high 15 minutes after failing over, page the network on-call team.

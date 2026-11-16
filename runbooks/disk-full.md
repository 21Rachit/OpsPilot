# Disk full on a server

Use this runbook when a disk is full or almost full. Writes start failing with
"No space left on device", databases stop accepting changes, and services crash.

## Symptoms

- Errors containing "No space left on device" or "disk quota exceeded"
- Postgres logs "could not extend file" and rejects inserts
- Alerts such as "disk usage above 90%"

## Diagnose

Find which filesystem is full, then what is using the space:

```bash
df -h
# biggest directories under /var, largest first
sudo du -xh /var | sort -rh | head -20
```

Deleted files that a process still holds open keep using space until the process restarts:

```bash
sudo lsof +L1
```

## Fix

### Free space quickly

1. Delete old compressed logs. They are safe to remove and are usually the biggest win:

   ```bash
   sudo find /var/log -name "*.gz" -mtime +7 -delete
   ```

2. Clean the package cache:

   ```bash
   sudo apt-get clean
   ```

3. If `lsof +L1` showed deleted files, restart the process that holds them, so the
   space is given back.

4. Remove unused Docker images and stopped containers, which often take several gigabytes
   on build servers:

   ```bash
   docker system prune -f
   ```

### Stop it happening again

Make sure log rotation runs daily and keeps at most 7 files. Add an alert at 80% usage so
there is time to act before writes fail. If the data really is growing, resize the volume.

## Escalate

If the disk belongs to a database and freeing space does not bring it back, page the
database on-call team. Do not delete files inside the database's data directory.

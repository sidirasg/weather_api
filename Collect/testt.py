from datetime import date, timedelta, datetime,timezone

now = datetime.now()
timestamp = datetime.timestamp(now)
dt = datetime.now(timezone.utc)
# timestamp=datetime.fromtimestamp(timestamp)
print("timestamp =", timestamp,"dt:",dt)

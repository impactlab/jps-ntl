#!/bin/sh
for meter in $(cat /home/tplagge/meterlist) ; do 
  echo $meter
  python /home/tplagge/jps-ntl/jps/manage.py shell <<EOF
from viewer import models
try:
  m = models.Meter.objects.get(meter_id="$meter")
except:
  m = models.Meter.objects.create(meter_id="$meter", user=models.User.objects.get(pk=2))
m._load_data()
EOF
done

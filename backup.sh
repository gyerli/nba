#!/bin/bash
# Location to place backups.
backup_dir="/home/ec2-user/work/nba/backup/"
#String to append to the name of the backup files
#backup_date=`date +%d-%m-%Y`
backup_date=`date +%Y%m%d`
#Numbers of days you want to keep copie of your databases
number_of_days=5
databases="nba"
for i in $databases; do
  if [ "$i" != "template0" ] && [ "$i" != "template1" ]; then
    echo Dumping $i to $backup_dir$i\_$backup_date.dmp
    pg_dump -U postgres -Fc $i > $backup_dir$i\_$backup_date.dmp
  fi
done
find $backup_dir -type f -prune -mtime +$number_of_days -exec rm -f {} \;

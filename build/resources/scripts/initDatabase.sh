#/bin/bash
# Prepares config file

if [ "$#" -ne "4" ]
then
  echo "Usage: `basename $0` dbUser dbPassword dbName sqlFileName"
  exit 1
fi

dir=`dirname $0`
base=`dirname $dir`
base=`dirname $base`
base=`dirname $base`

# Re-init database - drop all tables
tables=$(mysql -u $1 -p$2 $3 -e 'show tables' | awk '{ print $1}' | grep -v '^Tables' )

for t in $tables
do
        echo "Deleting $t table from $3 database..."
        mysql -u $1 -p$2 $3 -e "SET FOREIGN_KEY_CHECKS = 0;DROP TABLE IF EXISTS $t;SET FOREIGN_KEY_CHECKS = 1;"
done

# init structure - add all databases into one
mysql -u $1 -p$2 $3 < $base/sql/$4_structure.sql

echo Structure of $3 initialized from $base/sql/$4_structure.sql

# init data - add all databases into one
initFile=$base/sql/tests/$4_initial.sql

if [ -f $initFile ]
then
	mysql -u $1 -p$2 $3 < $initFile
	echo Data of $3 initialized from $initFile
fi

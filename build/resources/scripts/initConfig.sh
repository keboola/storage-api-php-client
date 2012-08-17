#/bin/bash
# Prepares config file
# TODO: fetch config from another repository insteadof copy and replace

if [ "$#" -ne "2" ]
then
  echo "Usage: `basename $0` dbUser dbPassword"
  exit 1
fi

dir=`dirname $0`
base=`dirname $dir`
base=`dirname $base`
base=`dirname $base`

configFile=$base/application/configs/config.ini

echo $dir
echo $base

cp "$base/application/configs/config.template.ini" "$configFile"

url="http:\/\/connection-jenkins-devel.keboola.com"
sed "s/keboolaStorageApi.url =/keboolaStorageApi.url = $url/" -i $configFile
sed "s/keboolaStorageApi.clientDbPrefix = sapi_/keboolaStorageApi.clientDbPrefix = jenkins_sapi_/" -i $configFile

sed "s/s3.awsAccessKey =/s3.awsAccessKey = AKIAJJKM4R26QUNBFPTA/" -i $configFile
sed "s/s3.awsSecretKey =/s3.awsSecretKey = wjanQcsL1P5bnJBbHfb7rxrbEZ\/7qDNa5Oma2z9O/" -i $configFile

sed "s/db.twitter.host =/db.twitter.host = localhost/" -i $configFile
sed "s/db.twitter.login =/db.twitter.login = $1/" -i $configFile
sed "s/db.twitter.password =/db.twitter.password = $2/" -i $configFile
sed "s/db.twitter.name =/db.twitter.name = jenkins_bi_twitter/" -i $configFile

sed "s/db.accounts.host =/db.accounts.host = localhost/" -i $configFile
sed "s/db.accounts.login =/db.accounts.login = $1/" -i $configFile
sed "s/db.accounts.password =/db.accounts.password = $2/" -i $configFile
sed "s/db.accounts.name =/db.accounts.name = jenkins_bi_accounts/" -i $configFile

echo "Configuration file $configFile created"
echo "Configuration file $testConfigFile created"

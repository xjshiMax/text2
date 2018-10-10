if [ "$(whereis mysql |wc -l)"=="0" ]
then
wget -i -c http://dev.mysql.com/get/mysql57-community-release-el7-10.noarch.rpm
yum -y install mysql57-community-release-el7-10.noarch.rpm
yum -y install mysql-community-server
else
echo "$(whereis mysql |wc -l)"
fi
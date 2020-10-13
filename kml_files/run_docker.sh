 #!/usr/bin/expect -f

M_WORKFLOW_ID=`date +"%Y%m%d_%H%M%S"`
FILE_PATH="/data/app_data/storage/kml_files"
EXPORT_PATH="$FILE_PATH/data/${M_WORKFLOW_ID}"
HOSTNAME="sftp.megafon.ru"


if [ -z ${1} ];
then
  M_VERSION='0.0.27'
else
  M_VERSION=${1}
fi

if [ -z ${2} ];
then
  M_RUN_DATE=$(date -d "`date +%Y%m01` -1 month" +%Y-%m-%d)
  M_NORMALIZED_RUN_DATE=$(echo $M_RUN_DATE | sed -r 's/[-]+//g')
  M_BEG_DATE=${M_NORMALIZED_RUN_DATE}
else
  M_BEG_DATE=${2}
fi

if [ -z ${3} ];
then
  M_DB_USER_=${M_DB_USER}
else
  M_DB_USER_=${3}
fi

if [ -z ${4} ];
then
  M_DB_PASSWORD_=`${M_DB_PASSWORD}`
else
  M_DB_PASSWORD_=${4}
fi

if [ -z ${5} ];
then
  ROLE_ID=${M_KML_FILES_FTP_ROLE_ID}
else
  ROLE_ID=${5}
fi

if [ -z ${6} ];
then
  SECRET_ID=${M_KML_FILES_FTP_SECRET_ID}
else
  SECRET_ID=${6}
fi

M_TOKEN=$(curl --request POST --data '{"role_id":"'$ROLE_ID'","secret_id":"'$SECRET_ID'"}' https://vault.megafon.ru:8200/v1/auth/approle/login | jq -r '.auth.client_token')
USERNAME_SFT=$(curl --header "X-Vault-Token: $M_TOKEN" https://vault.megafon.ru:8200/v1/iti/data/CI_CD%20для%20разработок%20BigData/prom/ksi/sftp | jq -r '.data.data.login')
PASSWORD_SFT=$(curl --header "X-Vault-Token: $M_TOKEN" https://vault.megafon.ru:8200/v1/iti/data/CI_CD%20для%20разработок%20BigData/prom/ksi/sftp | jq -r '.data.data.password')


 docker run --rm -it \
 -v $FILE_PATH/data/${M_WORKFLOW_ID}:/d/app_data/kml_files/export_kml/ \
 -v $FILE_PATH/logs/${M_WORKFLOW_ID}:/d/app_data/log_json/ \
 msk-hdp-dn171.megafon.ru:5000/megafon/kml_files:${M_VERSION} \
 ${M_BEG_DATE} \
 ${M_DB_USER_} \
 ${M_DB_PASSWORD_} 

spawn sftp $USERNAME_SFT@$HOSTNAME
expect "password:"
send "$PASSWORD_SFT\n"
expect "sftp>"
send "cd kml_files/"
expect "sftp>"
send "put -r $EXPORT_PATH\n"
expect "sftp>"
send "exit\n"

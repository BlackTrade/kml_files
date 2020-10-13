#!/bin/sh
R_D=$(date +"%Y-%m-%d_%H:%M:%S")
#R_DL=$(date +"%Y%m%d_%H%M%S")

M_MAIN_TASK_NAME="kml_files"

M_LOG_DIR="/d/app_data/log_json"
#mkdir $M_LOG_DIR

if [ -z ${1} ];
then
  M_RUN_DATE=$(date -d "`date +%Y%m01` -1 month" +%Y-%m-%d)
  M_NORMALIZED_RUN_DATE=$(echo $M_RUN_DATE | sed -r 's/[-]+//g')
  BEG_DATE=${M_NORMALIZED_RUN_DATE}
else
  BEG_DATE=${1}
fi

if [ -z ${2} ];
then
  ORACLE_USER=$ORACLE_USER
else
  ORACLE_USER=${2}
fi

if [ -z ${3} ];
then
  ORACLE_PASS=$ORACLE_PASS
else
  ORACLE_PASS=${3}
fi

if [ -z ${4} ];
then
   USERNAME_SFT="999"
else
   USERNAME_SFT=${4}
fi

if [ -z ${5} ];
then
   PASSWORD_SFT="999"
else
   PASSWORD_SFT=${5}
fi

if [ -z ${6} ];
then
  INPUT_TABLE="pub_ds.f_vigo_clusters_ext"
else
  INPUT_TABLE=${6}
fi

if [ -z ${7} ];
then
  TYPE_WORK="prod"
else
  TYPE_WORK=${7}
fi

if [ -z ${8} ];
then
  LOG_PATH=$M_LOG_DIR
else
  LOG_PATH=${8}
fi

M_MAIN_INFO_LOG=${LOG_PATH}/${M_MAIN_TASK_NAME}_info.log
M_MAIN_STD_LOG=${LOG_PATH}/${M_MAIN_TASK_NAME}_std_full.log
M_MAIN_ERR_LOG=${LOG_PATH}/${M_MAIN_TASK_NAME}_err_full.log
M_MAIN_STATUS_OK_LOG=${LOG_PATH}/${M_MAIN_TASK_NAME}_ok.log
M_MAIN_STATUS_ERR_LOG=${LOG_PATH}/${M_MAIN_TASK_NAME}_err.log

function log() {
  local date=`date "+%Y-%m-%d_%H:%M:%S"`
  local priority=$1
  local thread=$2
  local category=$3
  local message=$4
  echo "${date} ${priority} ${thread} ${category} - ${message}"
}

function checkExitCode(){
    echo "return code $1 for task $2"
    echo "return code $1 for task $2" >> $3
    if [ $1 != 0 ];then
      echo "Error while executing $2"
      echo "Error while executing $2" >> $4
    fi
    if [ $1 != 0 ];then
      exit $1
    fi
}

log "INFO" "main" ${M_MAIN_TASK_NAME} "Start" >> ${M_MAIN_INFO_LOG}

if [ ${TYPE_WORK} == 'prod' ];
then
    python /d/app_data/kml_files/main.py \
    -n ${R_D} \
    -b ${BEG_DATE} \
    -w ${TYPE_WORK} \
    -g ${LOG_PATH} \
    -u ${ORACLE_USER} \
    -p ${ORACLE_PASS} \
    -t ${INPUT_TABLE} \
    -f ${USERNAME_SFT} \
    -a ${PASSWORD_SFT} > ${M_MAIN_STD_LOG} 2> ${M_MAIN_ERR_LOG}
elif [ ${TYPE_WORK} == 'test' ];
then
    python /kml_files/main.py \
    -n ${R_D} \
    -b ${BEG_DATE} \
    -w ${TYPE_WORK} \
    -g ${LOG_PATH} \
    -u ${ORACLE_USER} \
    -p ${ORACLE_PASS} \
    -t ${INPUT_TABLE} \
    -f ${USERNAME_SFT} \
    -a ${PASSWORD_SFT}> ${M_MAIN_STD_LOG} 2> ${M_MAIN_ERR_LOG}
fi
#
EXIT_CODE=$?
log "INFO" "main" ${M_MAIN_TASK_NAME} "Finish" >> ${M_MAIN_INFO_LOG}

checkExitCode ${EXIT_CODE} ${M_MAIN_TASK_NAME} ${M_MAIN_STATUS_OK_LOG} ${M_MAIN_STATUS_ERR_LOG}

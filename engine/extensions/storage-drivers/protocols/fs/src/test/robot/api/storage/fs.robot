*** Settings ***
Documentation  Spt FS Storage Driver For Win Tests
Force Tags  FS
Library  Collections
Library  OperatingSystem
Library  Validation
Library  String
Test Teardown  Remove Files

*** Variables ***
${HOME_DIR}     C:\\Users\\appveyor
${ITEM_OUTPUT_PATH}     ${HOME_DIR}\\output
${ITEM_COUNT}    10
${ITEM_SIZE}    10KB
${STEP_ID}   win_fs_robotest

*** Test Cases ***
Should Create Files Windows Test
	${BASE_VERSION}    Get Environment Variable  BASE_VERSION
	Windows Create Directory  ${ITEM_OUTPUT_PATH}
	Windows Start Spt  ${BASE_VERSION}
	${log_dir} =  Windows Get Log Directory  ${BASE_VERSION}
	Validate Log File Metrics Total  ${log_dir}\\${STEP_ID}  file_separator=\\  count_succ_min=${10}  count_succ_max=${10}
	...  transfer_size=${10240000}  transfer_size_delta=${10240000}

*** Keywords ***
Windows Get Log Directory
	[Arguments]  ${version}
	${cmd} =  Catenate  ECHO %HomeDrive%%HomePath%
	${std_out} =  Run   ${cmd}
	${std_out} =  Strip String  ${std_out}
	[Return]  ${std_out}\\.spt\\${version}\\log

Windows Start Spt
	[Arguments]  ${base_version}
	${SPT_JAR_PATH} =  Catenate  ${HOME_DIR}\\spt-base-${base_version}.jar
	${java_home} =  Get Environment Variable  JAVA_HOME
	${cmd} =  Catenate  SEPARATOR=\t
	...  ${java_home}\\bin\\java -jar ${SPT_JAR_PATH}
	...  --item-data-size=${ITEM_SIZE}
	...  --storage-driver-type=fs
	...  --item-output-path=${ITEM_OUTPUT_PATH}
	...  --load-op-limit-count=${ITEM_COUNT}
	...  --load-step-id=${STEP_ID}
	${std_out} =  Run   ${cmd}
    Log  ${std_out}

Windows Create Directory
	[Arguments]  ${path}
	${cmd} =  Catenate  mkdir ${path}
	${std_out} =  Run   ${cmd}
    Log  ${std_out}

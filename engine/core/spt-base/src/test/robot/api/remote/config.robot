*** Settings ***
Documentation   Spt Config API tests
Force Tags      Config
Resource        ../../lib/Common.robot
Library         OperatingSystem
Library         RequestsLibrary



*** Variables ***
${DATA_DIR}                         src/test/robot/api/remote/data
${SPT_CONFIG_URI_PATH}         /config
${SPT_CONFIG_SCHEMA_URI_PATH}  ${SPT_CONFIG_URI_PATH}/schema



*** Test Cases ***
Should Return Aggregated Defaults
    Should Return Text  ${DATA_DIR}/aggregated_defaults.yaml  ${SPT_CONFIG_URI_PATH}

Should Return Aggregated Schema
    Should Return Text  ${DATA_DIR}/aggregated_defaults_schema.yaml  ${SPT_CONFIG_SCHEMA_URI_PATH}



*** Keywords ***
Should Return Text
    [Arguments]  ${expected_file_name}  ${uri_path}
    ${expected_text} =  Get File  ${expected_file_name}
    ${resp} =  Get On Session  ${SESSION_NAME}  ${uri_path}
    Should Be Equal As Strings  ${resp.status_code}  200
    Should Be Equal  ${expected_text}  ${resp.text}

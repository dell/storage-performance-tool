*** Settings ***
Documentation   Spt Logs API tests
Force Tags      Logs
Resource        ../../lib/Common.robot
Library         Collections
Library         OperatingSystem
Library         RequestsLibrary
Library         String


*** Variables ***
${MESS_LOGGER_NAME}         Messages
${OP_TRACE_LOGGER_NAME}     OpTraces
${SPT_LOGS_URI_PATH}   /logs



*** Test Cases ***
Should Respond Message Logs
    ${data} =  Make Start Request Payload
    Sleep  10s
    ${resp_start} =  Start Spt Scenario  ${data}
    ${resp_etag_header} =  Get From Dictionary  ${resp_start.headers}  ${HEADER_ETAG}
    ${uri_path} =  Catenate  ${SPT_LOGS_URI_PATH}/${STEP_ID}/${MESS_LOGGER_NAME}
    Wait Until Keyword Succeeds  5x  7s  Should Return Status  ${uri_path}  200
    ${resp} =  Get On Session  ${SESSION_NAME}  ${uri_path}
    Should Be Equal As Strings  ${resp.status_code}  200
    Should Include String  ${resp.text}  *| INFO |*
    ${resp_stop} =  Stop Spt Scenario Run  ${resp_etag_header}

Should Respond Operation Trace Logs
    ${data} =  Make Start Request Payload
    ${resp_start} =  Start Spt Scenario  ${data}
    ${resp_etag_header} =  Get From Dictionary  ${resp_start.headers}  ${HEADER_ETAG}
    ${uri_path} =  Catenate  ${SPT_LOGS_URI_PATH}/${STEP_ID}/${OP_TRACE_LOGGER_NAME}
    Wait Until Keyword Succeeds  5x  7s  Should Return Status  ${uri_path}  200
    ${resp} =  Get On Session  ${SESSION_NAME}  ${uri_path}
    Should Be Equal As Strings  ${resp.status_code}  200
    Should Include String  ${resp.text}  *
    ${resp_stop} =  Stop Spt Scenario Run  ${resp_etag_header}

Should Delete Logs
    ${data} =  Make Start Request Payload
    ${resp_start} =  Start Spt Scenario  ${data}
    ${resp_etag_header} =  Get From Dictionary  ${resp_start.headers}  ${HEADER_ETAG}
    ${uri_path} =  Catenate  ${SPT_LOGS_URI_PATH}/${STEP_ID}/${MESS_LOGGER_NAME}
    Wait Until Keyword Succeeds  5x  7s  Should Return Status  ${uri_path}  200
    Delete On Session  ${SESSION_NAME}  ${uri_path}
    ${resp} =  Get On Session  ${SESSION_NAME}  ${uri_path}  expected_status=404
    Should Be Equal As Strings  ${resp.status_code}  404
    ${resp_stop} =  Stop Spt Scenario Run  ${resp_etag_header}

Should Respond Loggers
    ${expected_text} =  Get File  ${DATA_DIR}/loggers.json
    ${resp} =  Get On Session  ${SESSION_NAME}  ${SPT_LOGS_URI_PATH}
    Should Be Equal As Strings  ${resp.status_code}  200
    Should Be Equal  ${expected_text}  ${resp.text}



*** Keywords ***
Make Start Request Payload
    ${defaults_data} =  Get Binary File  ${DATA_DIR}/logs_test_defaults.yaml
    ${scenario_data} =  Get Binary File  ${DATA_DIR}/scenario_dummy.js
    &{data} =  Create Dictionary  defaults=${defaults_data}  scenario=${scenario_data}
    [Return]  ${data}


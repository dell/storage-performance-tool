*** Settings ***
Documentation  Commons Keywords



*** Variables ***
${DATA_DIR}                src/test/robot/api/remote/data
${SPT_RUN_URI_PATH}   /run
${SPT_REST_PORT}      9999
${SPT_RMI_PORT}       1099
${SPT_ADD_REST_PORT}  9990
${SPT_ADD_RMI_PORT}   1098
${SESSION_NAME}            spt_node
${ADD_SESSION_NAME}        spt_add_node
${STEP_ID}                 robotest
${HEADER_ETAG}             ETag



*** Keywords ***
Start Spt Scenario
    [Arguments]  ${data}
    ${resp} =  POST Request  ${SESSION_NAME}  ${SPT_RUN_URI_PATH}  files=${data}
    Log  ${resp.status_code}
    [Return]  ${resp}

Stop Spt Scenario Run
    [Arguments]  ${etag}
    &{req_headers} =  Create Dictionary  If-Match=${etag}
    ${resp} =  Delete On Session  ${SESSION_NAME}  ${SPT_RUN_URI_PATH}  headers=${req_headers}
    Log  ${resp.status_code}
    [Return]  ${resp}

Should Return Status
    [Arguments]  ${uri_path}  ${expected_status}
    ${resp} =  Get On Session  ${SESSION_NAME}  ${uri_path}
    Should Be Equal As Strings  ${resp.status_code}  ${expected_status}

Should Include String
    [Arguments]  ${result}  ${pattern}
    ${lines} =    Get Lines Matching Pattern    ${result}    ${pattern}
    ${count} =  Get Line Count  ${lines}
    Should Be True  ${count}>0

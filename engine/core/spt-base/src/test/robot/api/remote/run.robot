*** Settings ***
Documentation   Spt Run API tests
Force Tags      Run
Resource        ../../lib/Common.robot
Resource        ../../lib/SptContainer.robot
Library         Collections
Library         OperatingSystem
Library         RequestsLibrary
Library         String


*** Variables ***
${HEADER_IF_MATCH}   If-Match



*** Test Cases ***

Should Stop Running Scenario In Distributed Mode
    ${data} =  Make Start Request Payload For Distributed Mode
    Sleep  10s
    ${resp_start} =  Start Spt Scenario  ${data}
    Get Docker Logs From Container With Name ${SESSION_NAME}
    ${resp_etag_header} =  Get From Dictionary  ${resp_start.headers}  ${HEADER_ETAG}
    Sleep  12s
    ${resp_stop} =  Stop Spt Scenario Run  ${resp_etag_header}
    Log  ${resp_stop.text}
    Should Be Equal As Strings  ${resp_stop.status_code}  200
    Should Not Export Metrics More Then 10s
    Get Docker Logs From Container With Name ${SESSION_NAME}
    Get Docker Logs From Container With Name ${ADD_SESSION_NAME}

Should Start Scenario
    ${data} =  Make Start Request Payload Full
    ${resp_start} =  Start Spt Scenario  ${data}
    Log  ${resp_start}
    Should Be Equal As Strings  ${resp_start.status_code}  202
    ${resp_etag_header} =  Get From Dictionary  ${resp_start.headers}  ${HEADER_ETAG}
    ${resp_stop} =  Stop Spt Scenario Run  ${resp_etag_header}

Should Start With Implicit Default Scenario
    ${data} =  Make Start Request Payload Without Scenario Part
    ${resp_start} =  Start Spt Scenario  ${data}
    Should Be Equal As Strings  ${resp_start.status_code}  202
    ${resp_etag_header} =  Get From Dictionary  ${resp_start.headers}  ${HEADER_ETAG}
    ${resp_stop} =  Stop Spt Scenario Run  ${resp_etag_header}

Should Start With Implicit Default Scenario And Partial Config
    ${data} =  Make Start Request Payload With Partial Config
    ${resp_start} =  Start Spt Scenario  ${data}
    Should Be Equal As Strings  ${resp_start.status_code}  202
    ${resp_etag_header} =  Get From Dictionary  ${resp_start.headers}  ${HEADER_ETAG}
    ${resp_stop} =  Stop Spt Scenario Run  ${resp_etag_header}

Should Stop Running Scenario
    ${data} =  Make Start Request Payload Full
    ${resp_start} =  Start Spt Scenario  ${data}
    ${resp_etag_header} =  Get From Dictionary  ${resp_start.headers}  ${HEADER_ETAG}
    ${resp_stop} =  Stop Spt Scenario Run  ${resp_etag_header}
    Should Be Equal As Strings  ${resp_stop.status_code}  200

Should Stop Running Scenario After Error In Distributed Mode
    ${data} =  Make Start Request Payload Invalid For Distributed Mode
    ${resp_start} =  Start Spt Scenario  ${data}
    Should Be Equal As Strings  ${resp_start.status_code}  202
    Should Stop Logging Errors After 5s

Should Not Stop Not Running Scenario
    ${data} =  Make Start Request Payload Full
    ${resp_start} =  Start Spt Scenario  ${data}
    ${resp_etag_header} =  Get From Dictionary  ${resp_start.headers}  ${HEADER_ETAG}
    ${resp_stop_running} =  Stop Spt Scenario Run  ${resp_etag_header}
    Should Be Equal As Strings  ${resp_stop_running.status_code}  200
    ${resp_stop_stopped} =  Stop Spt Scenario Run  ${resp_etag_header}
    Should Be Equal As Strings  ${resp_stop_stopped.status_code}  204

Should Not Start Scenario With Invalid Defaults
    ${data} =  Make Start Request Payload Invalid
    ${resp_start} =  Start Spt Scenario  ${data}
    Should Be Equal As Strings  ${resp_start.status_code}  400

Should Return The Node State
    ${data} =  Make Start Request Payload Full
    ${resp_status_running} =  Get Spt Node Status
    ${resp_start} =  Start Spt Scenario  ${data}
    ${resp_etag_header} =  Get From Dictionary  ${resp_start.headers}  ${HEADER_ETAG}
    ${resp_status_running} =  Get Spt Node Status
    Should Be Equal As Strings  ${resp_status_running.status_code}  200
    ${resp_status_etag_header} =  Get From Dictionary  ${resp_status_running.headers}  ${HEADER_ETAG}
    Should Be Equal As Strings  ${resp_etag_header}  ${resp_status_etag_header}
    ${resp_stop} =  Stop Spt Scenario Run  ${resp_etag_header}
    Should Be Equal As Strings  ${resp_stop.status_code}  200
    ${resp_status_stopped} =  Get Spt Node Status
    Should Be Equal As Strings  ${resp_status_stopped.status_code}  204

Should Return Scenario Run State
    ${data} =  Make Start Request Payload Full
    ${resp_start} =  Start Spt Scenario  ${data}
    ${resp_etag_header} =  Get From Dictionary  ${resp_start.headers}  ${HEADER_ETAG}
    Should Return Spt Scenario Run State  ${resp_etag_header}  200
    ${resp_stop} =  Stop Spt Scenario Run  ${resp_etag_header}
    Wait Until Keyword Succeeds  5x  7s  Should Return Spt Scenario Run State  ${resp_etag_header}  204



*** Keywords ***
Make Start Request Payload Full
    ${defaults_data} =  Get Binary File  ${DATA_DIR}/aggregated_defaults.yaml
    ${scenario_data} =  Get Binary File  ${DATA_DIR}/scenario_dummy.js
    &{data} =  Create Dictionary  defaults=${defaults_data}  scenario=${scenario_data}
    [Return]  ${data}

Make Start Request Payload For Distributed Mode
    ${defaults_data} =  Get Binary File  ${DATA_DIR}/distributed_defaults.yaml
    &{data} =  Create Dictionary  defaults=${defaults_data}
    [Return]  ${data}

Make Start Request Payload Invalid For Distributed Mode
    ${defaults_data} =  Get Binary File  ${DATA_DIR}/distributed_defaults_invalid.yaml
    &{data} =  Create Dictionary  defaults=${defaults_data}
    [Return]  ${data}

Make Start Request Payload Invalid
    ${defaults_data} =  Get Binary File  ${DATA_DIR}/aggregated_defaults_invalid.yaml
    ${scenario_data} =  Get Binary File  ${DATA_DIR}/scenario_dummy.js
    &{data} =  Create Dictionary  defaults=${defaults_data}  scenario=${scenario_data}
    [Return]  ${data}

Make Start Request Payload Without Scenario Part
    ${defaults_data} =  Get Binary File  ${DATA_DIR}/aggregated_defaults.yaml
    &{data} =  Create Dictionary  defaults=${defaults_data}
    [Return]  ${data}

Make Start Request Payload With Partial Config
    ${defaults_data} =  Get Binary File  ${DATA_DIR}/partial_defaults.yaml
    &{data} =  Create Dictionary  defaults=${defaults_data}
    [Return]  ${data}

Should Return Spt Scenario Run State
    [Arguments]  ${etag}  ${expected_status_code}
    ${resp_state} =  Get Spt Scenario Run State  ${etag}
    Should Be Equal As Strings  ${resp_state.status_code}  ${expected_status_code}

Get Spt Node Status
    ${resp} =  Head On Session  ${SESSION_NAME}  ${SPT_RUN_URI_PATH}
    Log  ${resp.status_code}
    [Return]  ${resp}

Get Spt Scenario Run State
    [Arguments]  ${etag}
    &{req_headers} =  Create Dictionary  If-Match=${etag}
    ${resp} =  Get On Session  ${SESSION_NAME}  ${SPT_RUN_URI_PATH}  headers=${req_headers}
    Log  ${resp.status_code}
    [Return]  ${resp}

Should Not Export Metrics More Then ${time}
    Sleep  ${time}
    ${text1}   Get Metrics File Content
    ${text1_lines_count} =  Get Line Count  ${text1}
    Sleep  12s
    ${text2}   Get Metrics File Content
    ${text2_lines_count} =  Get Line Count  ${text2}
    Should Be Equal As Strings  ${text1_lines_count}  ${text2_lines_count}

Should Stop Logging Errors After ${time}
    Sleep  ${time}
    ${text1}   Get Error Log File Content
    ${text1_lines_count} =  Get Line Count  ${text1}
    Sleep  12s
    ${text2}   Get Error Log File Content
    ${text2_lines_count} =  Get Line Count  ${text2}
    Should Be Equal As Strings  ${text1_lines_count}  ${text2_lines_count}

Get Metrics File Content
    ${uri_path} =  Catenate  /logs/${STEP_ID}/metrics.File
    Wait Until Keyword Succeeds  5x  5s  Should Return Status  ${uri_path}  200
    ${resp} =  Get On Session  ${SESSION_NAME}  ${uri_path}
    Should Be Equal As Strings  ${resp.status_code}  200
    [Return]  ${resp.text}

Get Error Log File Content
    ${uri_path} =  Catenate  /logs/${STEP_ID}/Errors
    Wait Until Keyword Succeeds  5x  5s  Should Return Status  ${uri_path}  200
    ${resp} =  Get On Session  ${SESSION_NAME}  ${uri_path}
    Should Be Equal As Strings  ${resp.status_code}  200
    [Return]  ${resp.text}

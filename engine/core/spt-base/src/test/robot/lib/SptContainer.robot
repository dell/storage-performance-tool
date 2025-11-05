*** Settings ***
Documentation  Spt Container Keywords
Resource       Common.robot
Library        OperatingSystem
Library        RequestsLibrary



*** Variables ***
${LOG_DIR}                      build/log
${SPT_CONTAINER_DATA_DIR}  /data
${SPT_CONTAINER_NAME}      spt
${SPT_IMAGE_NAME}          dcr.octo.dell.com/spt/spt-base



*** Keywords ***
Start Spt Nodes
    Start Entry Spt Node
    Start Additional Spt Node

Start Entry Spt Node
    ${network} =  Catenate  --network host
    Start Spt Node  ${SESSION_NAME}  ${SPT_REST_PORT}  ${network}

Start Additional Spt Node
    ${network} =  Catenate  -p ${SPT_ADD_RMI_PORT}:${SPT_RMI_PORT} -p ${SPT_ADD_REST_PORT}:${SPT_REST_PORT}
    Start Spt Node  ${ADD_SESSION_NAME}  ${SPT_ADD_REST_PORT}  ${network}

Remove Spt Nodes
    Delete All Sessions
    Run  docker stop ${SESSION_NAME}
    Run  docker rm ${SESSION_NAME}
    Run  docker stop ${ADD_SESSION_NAME}
    Run  docker rm ${ADD_SESSION_NAME}

Start Spt Node
    [Arguments]  ${session_name}  ${rest_port}  ${network}
    ${image_version} =  Get Environment Variable  SPT_IMAGE_VERSION
    # ${service_host} should be used instead of the "localhost" in GL CI
    ${service_host} =  Get Environment Variable  SERVICE_HOST
    ${cmd} =  Catenate  SEPARATOR= \\\n\t
    ...  docker run
    ...  --detach
    ...  --name ${session_name}
    ...  ${network}
    ...  ${SPT_IMAGE_NAME}:${image_version}
    ...  --load-step-id=${STEP_ID}
    ...  --run-node
    ${std_out} =  Run  ${cmd}
    Log  ${std_out}
    Create Session  ${session_name}  http://${service_host}:${rest_port}  debug=1  timeout=8000  max_retries=40  retry_status_list=[405]
    Get Docker Logs From Container With Name ${session_name}

Execute Spt Scenario
    [Arguments]  ${shared_data_dir}  ${env}  ${args}
    ${docker_env_vars} =  Evaluate  ' '.join(['-e %s=%s' % (key, value) for (key, value) in ${env}.items()])
    ${host_working_dir} =  Get Environment Variable  HOST_WORKING_DIR
    Log  Host working dir: ${host_working_dir}
    ${spt_version} =  Get Environment Variable  SPT_VERSION
    ${image_version} =  Get Environment Variable  SPT_IMAGE_VERSION
    ${cmd} =  Catenate  SEPARATOR= \\\n\t
    ...  docker run
    ...  --name ${SPT_CONTAINER_NAME}
    ...  --network host
    ...  ${docker_env_vars}
    ...  --volume ${host_working_dir}/${shared_data_dir}:${SPT_CONTAINER_DATA_DIR}
    ...  --volume ${host_working_dir}/${LOG_DIR}:/root/.spt/${spt_version}/log
    ...  ${SPT_IMAGE_NAME}:${image_version}
    ...  ${args}
    ${std_out} =  Run  ${cmd}
    [Return]  ${std_out}


Get Docker Logs From Container With Name ${name}
     ${cmd} =  Catenate  docker logs ${name}
     ${std_out} =  Run  ${cmd}
     Log  ${std_out}

Get Internal IP Of Docker Container With Name ${name}
    ${cmd} =  Catenate  docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' ${name}
    ${std_out} =  Run  ${cmd}
    [Return]  ${std_out}

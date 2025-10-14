*** Settings ***
Documentation  Spt Container Keywords
Library  OperatingSystem
Library  RequestsLibrary

*** Variables ***
${LOG_DIR} =  build/log
${SPT_CONTAINER_DATA_DIR} =  /data
${SPT_CONTAINER_NAME} =  spt
${SPT_IMAGE_NAME} =  dellspt/spt-storage-driver-s3
${SPT_NODE_PORT} =  9999

*** Keywords ***
Start Spt Node
    ${image_version} =  Get Environment Variable  SPT_IMAGE_VERSION
    # ${service_host} should be used instead of the "localhost" in GL CI
    ${service_host} =  Get Environment Variable  SERVICE_HOST
    ${cmd} =  Catenate  SEPARATOR= \\\n\t
    ...  docker run
    ...  --detach
    ...  --name ${SPT_CONTAINER_NAME}
    ...  --publish ${SPT_NODE_PORT}:${SPT_NODE_PORT}
    ...  ${SPT_IMAGE_NAME}:${image_version}
    ...  --load-step-id=robotest
    ...  --run-node
    ${std_out} =  Run  ${cmd}
    Log  ${std_out}
    Create Session  spt_node  http://${service_host}:${SPT_NODE_PORT}  debug=1  timeout=1000  max_retries=10

Execute Spt Scenario
    [Arguments]  ${shared_data_dir}  ${env}  ${args}
    ${docker_env_vars} =  Evaluate  ' '.join(['-e %s=%s' % (key, value) for (key, value) in ${env}.items()])
    ${host_working_dir} =  Get Environment Variable  HOST_WORKING_DIR
    Log  Host working dir: ${host_working_dir}
    ${base_version} =  Get Environment Variable  BASE_VERSION
    ${image_version} =  Get Environment Variable  VERSION
    ${cmd} =  Catenate  SEPARATOR= \\\n\t
    ...  docker run
    ...  --name ${SPT_CONTAINER_NAME}
    ...  --network host
    ...  ${docker_env_vars}
    ...  --volume ${host_working_dir}/${shared_data_dir}:${SPT_CONTAINER_DATA_DIR}
    ...  --volume ${host_working_dir}/${LOG_DIR}:/root/.spt/${base_version}/log
    ...  ${SPT_IMAGE_NAME}:${image_version}
    ...  ${args}
    ${std_out} =  Run  ${cmd}
    [Return]  ${std_out}

Remove Spt Node
    Delete All Sessions
    Run  docker stop ${SPT_CONTAINER_NAME}
    Remove Spt Container

Remove Spt Container
    Run  docker rm ${SPT_CONTAINER_NAME}

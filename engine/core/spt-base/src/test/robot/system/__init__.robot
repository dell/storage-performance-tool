*** Settings ***
Documentation  Spt Limitations tests
Force Tags      Limitations
Resource        ../lib/SptContainer.robot
Library         OperatingSystem
Suite Setup     Start Spt Nodes
Suite Teardown  Remove Spt Nodes

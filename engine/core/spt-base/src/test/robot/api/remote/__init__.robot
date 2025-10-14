*** Settings ***
Documentation   Spt Remote API suite
Force Tags      Remote API
Resource        ../../lib/SptContainer.robot
Library         OperatingSystem
Suite Setup     Start Spt Nodes
Suite Teardown  Remove Spt Nodes


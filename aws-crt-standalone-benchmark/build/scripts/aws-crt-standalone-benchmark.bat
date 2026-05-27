@rem
@rem Copyright 2015 the original author or authors.
@rem
@rem Licensed under the Apache License, Version 2.0 (the "License");
@rem you may not use this file except in compliance with the License.
@rem You may obtain a copy of the License at
@rem
@rem      https://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing, software
@rem distributed under the License is distributed on an "AS IS" BASIS,
@rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem See the License for the specific language governing permissions and
@rem limitations under the License.
@rem
@rem SPDX-License-Identifier: Apache-2.0
@rem

@if "%DEBUG%"=="" @echo off
@rem ##########################################################################
@rem
@rem  aws-crt-standalone-benchmark startup script for Windows
@rem
@rem ##########################################################################

@rem Set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" setlocal

set DIRNAME=%~dp0
if "%DIRNAME%"=="" set DIRNAME=.
@rem This is normally unused
set APP_BASE_NAME=%~n0
set APP_HOME=%DIRNAME%..

@rem Resolve any "." and ".." in APP_HOME to make it shorter.
for %%i in ("%APP_HOME%") do set APP_HOME=%%~fi

@rem Add default JVM options here. You can also use JAVA_OPTS and AWS_CRT_STANDALONE_BENCHMARK_OPTS to pass JVM options to this script.
set DEFAULT_JVM_OPTS="-Xmx2g"

@rem Find java.exe
if defined JAVA_HOME goto findJavaFromJavaHome

set JAVA_EXE=java.exe
%JAVA_EXE% -version >NUL 2>&1
if %ERRORLEVEL% equ 0 goto execute

echo. 1>&2
echo ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH. 1>&2
echo. 1>&2
echo Please set the JAVA_HOME variable in your environment to match the 1>&2
echo location of your Java installation. 1>&2

goto fail

:findJavaFromJavaHome
set JAVA_HOME=%JAVA_HOME:"=%
set JAVA_EXE=%JAVA_HOME%/bin/java.exe

if exist "%JAVA_EXE%" goto execute

echo. 1>&2
echo ERROR: JAVA_HOME is set to an invalid directory: %JAVA_HOME% 1>&2
echo. 1>&2
echo Please set the JAVA_HOME variable in your environment to match the 1>&2
echo location of your Java installation. 1>&2

goto fail

:execute
@rem Setup the command line

set CLASSPATH=%APP_HOME%\lib\aws-crt-standalone-benchmark.jar;%APP_HOME%\lib\s3-2.25.0.jar;%APP_HOME%\lib\aws-crt-client-2.25.0.jar;%APP_HOME%\lib\slf4j-simple-2.0.9.jar;%APP_HOME%\lib\aws-xml-protocol-2.25.0.jar;%APP_HOME%\lib\aws-query-protocol-2.25.0.jar;%APP_HOME%\lib\protocol-core-2.25.0.jar;%APP_HOME%\lib\aws-core-2.25.0.jar;%APP_HOME%\lib\auth-2.25.0.jar;%APP_HOME%\lib\regions-2.25.0.jar;%APP_HOME%\lib\sdk-core-2.25.0.jar;%APP_HOME%\lib\arns-2.25.0.jar;%APP_HOME%\lib\profiles-2.25.0.jar;%APP_HOME%\lib\crt-core-2.25.0.jar;%APP_HOME%\lib\http-auth-2.25.0.jar;%APP_HOME%\lib\http-auth-aws-2.25.0.jar;%APP_HOME%\lib\http-auth-spi-2.25.0.jar;%APP_HOME%\lib\identity-spi-2.25.0.jar;%APP_HOME%\lib\apache-client-2.25.0.jar;%APP_HOME%\lib\netty-nio-client-2.25.0.jar;%APP_HOME%\lib\http-client-spi-2.25.0.jar;%APP_HOME%\lib\metrics-spi-2.25.0.jar;%APP_HOME%\lib\json-utils-2.25.0.jar;%APP_HOME%\lib\utils-2.25.0.jar;%APP_HOME%\lib\slf4j-api-2.0.9.jar;%APP_HOME%\lib\checksums-2.25.0.jar;%APP_HOME%\lib\checksums-spi-2.25.0.jar;%APP_HOME%\lib\endpoints-spi-2.25.0.jar;%APP_HOME%\lib\annotations-2.25.0.jar;%APP_HOME%\lib\aws-crt-0.29.9.jar;%APP_HOME%\lib\reactive-streams-1.0.4.jar;%APP_HOME%\lib\eventstream-1.0.1.jar;%APP_HOME%\lib\third-party-jackson-core-2.25.0.jar;%APP_HOME%\lib\httpclient-4.5.13.jar;%APP_HOME%\lib\httpcore-4.4.13.jar;%APP_HOME%\lib\commons-codec-1.15.jar;%APP_HOME%\lib\netty-codec-http2-4.1.107.Final.jar;%APP_HOME%\lib\netty-codec-http-4.1.107.Final.jar;%APP_HOME%\lib\netty-handler-4.1.107.Final.jar;%APP_HOME%\lib\netty-codec-4.1.107.Final.jar;%APP_HOME%\lib\netty-transport-classes-epoll-4.1.107.Final.jar;%APP_HOME%\lib\netty-transport-native-unix-common-4.1.107.Final.jar;%APP_HOME%\lib\netty-transport-4.1.107.Final.jar;%APP_HOME%\lib\netty-buffer-4.1.107.Final.jar;%APP_HOME%\lib\netty-resolver-4.1.107.Final.jar;%APP_HOME%\lib\netty-common-4.1.107.Final.jar;%APP_HOME%\lib\commons-logging-1.2.jar


@rem Execute aws-crt-standalone-benchmark
"%JAVA_EXE%" %DEFAULT_JVM_OPTS% %JAVA_OPTS% %AWS_CRT_STANDALONE_BENCHMARK_OPTS%  -classpath "%CLASSPATH%" com.example.benchmark.Main %*

:end
@rem End local scope for the variables with windows NT shell
if %ERRORLEVEL% equ 0 goto mainEnd

:fail
rem Set variable AWS_CRT_STANDALONE_BENCHMARK_EXIT_CONSOLE if you need the _script_ return code instead of
rem the _cmd.exe /c_ return code!
set EXIT_CODE=%ERRORLEVEL%
if %EXIT_CODE% equ 0 set EXIT_CODE=1
if not ""=="%AWS_CRT_STANDALONE_BENCHMARK_EXIT_CONSOLE%" exit %EXIT_CODE%
exit /b %EXIT_CODE%

:mainEnd
if "%OS%"=="Windows_NT" endlocal

:omega

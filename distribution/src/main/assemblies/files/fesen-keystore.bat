@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set ES_MAIN_CLASS=org.codelibs.fesen.common.settings.KeyStoreCli
set ES_ADDITIONAL_CLASSPATH_DIRECTORIES=lib/tools/keystore-cli
call "%~dp0fesen-cli.bat" ^
  %%* ^
  || goto exit

endlocal
endlocal
:exit
exit /b %ERRORLEVEL%

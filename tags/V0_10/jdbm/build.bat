@echo off
echo BUILDING JDBM
REM $Id: build.bat,v 1.1 2000/05/05 23:59:52 boisvert Exp $
set JAVA=%JAVA_HOME%\bin\java
set cp=%CLASSPATH%
for %%i in (lib\*.jar) do call cp.bat %%i
set CP=%JAVA_HOME%\lib\tools.jar;%CP%
%JAVA% -classpath %CP% -Dant.home=lib org.apache.tools.ant.Main %1 %2 %3 %4 %5 %6 -buildfile src\build.xml

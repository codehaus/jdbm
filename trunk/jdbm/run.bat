@echo off
REM $Id: run.bat,v 1.1 2003/03/21 03:19:35 boisvert Exp $
set JAVA=%JAVA_HOME%\bin\java
set cp=%CLASSPATH%
for %%i in (lib\*.jar) do call cp.bat %%i
set CP=%JAVA_HOME%\lib\tools.jar;build\classes;build\examples;%CP%
rem %JAVA% -Xrunhprof:cpu=samples,depth=10 -classpath %CP% %1 %2 %3 %4 %5 %6
%JAVA% -classpath %CP% %1 %2 %3 %4 %5 %6


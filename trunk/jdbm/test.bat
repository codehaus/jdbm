@echo off
REM $Id: test.bat,v 1.1 2003/10/31 00:09:10 dranatunga Exp $

:handleHelp
    IF "%1"=="-h" GOTO showUsage
    IF "%1"=="/h" GOTO showUsage
    IF "%1"=="/?" GOTO showUsage
    IF "%1"=="-help" GOTO showUsage

:selectRunner
    REM Default runner
    SET TESTUI=junit.textui.TestRunner
    IF "%1"=="-text"  goto useTextRunner
    IF "%1"=="-awt"   goto useAwtRunner
    IF "%1"=="-swing" goto useSwingRunner

:useTextRunner
    SET TESTUI=junit.textui.TestRunner
    SHIFT
    GOTO selectTest
    
:useAwtRunner
    SET TESTUI=junit.ui.TestRunner
    SHIFT
    GOTO selectTest
    
:useSwingRunner
    SET TESTUI=junit.swingui.TestRunner
    SHIFT
    GOTO selectTest
    
:selectTest
    REM Default test
    SET TESTCASE=jdbm.AllTests

:ensureTestsCompiled
    IF NOT EXIST build\tests call build.bat tests
    
:runJUnit
    ECHO Running %TESTUI% %1 %2 %3 %4 %5 %6 %TESTCASE%
    CALL run.bat %TESTUI% %1 %2 %3 %4 %5 %6 %TESTCASE%

:cleanResidue
    ECHO Cleaning junk left behind
    IF EXIST test.db erase test.db
    GOTO scriptEnd
    
:showUsage
    ECHO USAGE: test [-text^|-awt^|-swing] [testclass] [options]
    GOTO scriptEnd

:scriptEnd
    ECHO Finished running tests

#!/bin/sh

# $Id: test.sh,v 1.1 2000/05/05 23:59:52 boisvert Exp $

if [ -z "$JAVA_HOME" ] ; then
  JAVA=`which java`
  if [ -z "$JAVA" ] ; then
    echo "Cannot find JAVA. Please set your PATH."
    exit 1
  fi
  JAVA_BIN=`dirname $JAVA`
  JAVA_HOME=$JAVA_BIN/..
fi

JAVA=$JAVA_HOME/bin/java

CLASSPATH=`echo lib/*.jar | tr ' ' ':'`:$CLASSPATH
CLASSPATH=$JAVA_HOME/lib/tools.jar:$CLASSPATH
CLASSPATH=$CLASSPATH:./build/classes
CLASSPATH=$CLASSPATH:./build/tests

# Text-based UI:
# TESTUI=junit.textui.TestRunner

# AWT-based UI:
# TESTUI=junit.ui.TestRunner

# Swing-based UI:
TESTUI=junit.swingui.TestRunner

$JAVA -classpath $CLASSPATH $TESTUI "$@"

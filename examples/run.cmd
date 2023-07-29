@echo off

:: Ensure we have at least one parameter (the class name)
IF "%~1"=="" (
    echo Usage: %~nx0 [--all ^| full.class.Name] [-jvm-option]...
    echo    --all            run all examples.
    echo    full.class.Name  fully qualified class name to run.
    echo    -jvm-option      options and values passed to the java command.
    exit /b
)

:: Build the classpath
setlocal enabledelayedexpansion
for /r target\libs %%g in (*.jar) do (
    set CLASSPATH=!CLASSPATH!;%%g
)
set CLASSPATH=target\classes%CLASSPATH%

:: Run the java processes
if /I "%~1"=="--all" (
    for /r target\classes %%g in (*Job.class) do (
        set "file=%%~g"
        setlocal enabledelayedexpansion
        set "class=!file:target\classes\=!"
        set "class=!class:\=.!"
        set "class=!class:.class=!"
        echo =======================================================================
        echo Running !class!
        echo =======================================================================
        java %*:~2 -cp "!CLASSPATH!" "!class!"
        endlocal
    )
) else (
    echo =======================================================================
    echo Running %~1
    echo =======================================================================
    java %*:~2 -cp "%CLASSPATH%" "%~1"
)

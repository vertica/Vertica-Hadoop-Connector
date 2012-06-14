#! /bin/sh
# $PostgreSQL: pgsql/src/test/regress/pg_regress.sh,v 1.53 2005/01/15 04:15:51 tgl Exp $

me=`basename $0`
: ${TMPDIR=/tmp}
TMPFILE=$TMPDIR/test_driver.$$
# version of diff to detect differences
: ${DIFF=diff --text}
# version of diff to report differences
: ${DIF2=$DIFF}

help="\
Vertica regression test driver

Usage: $me [options...] [extra tests...]

Options:
  --debug                   turn on debug mode in programs that are run
  --inputdir=DIR            take input files from DIR (default \`.')
  --outputdir=DIR           place output files in DIR (default \`.')
  --schedule=FILE           use test ordering schedule from FILE
                            (may be used multiple times to concatenate)

Options for using an existing installation:
  --host=HOST               use postmaster running on HOST
  --port=PORT               use postmaster running at PORT
  --user=USER               connect as USER

Using the 'cd:' command in a schedule (dot in) file:

  The 'cd:' command can be used to navigate the test tree.  The 'cd:'
  command changes the current working directory of $me to the path
  given by its argument.  The argument path is relative to the value of
  inputdir (SQLTest).  Without an argument the 'cd:' command chdirs back
  to the value of the inputdir.  The expected and results directories are
  set according to the arg of the 'cd:' command as well.

Using the 'testfilters:' command in a schedule (dot in) file:

  Tests can be filtered based upon values found in the TESTFILTERS environment
  variable and the testfilters command args in the schedule (dot in) file.
  If one of the testfilters args match what's in TESTFILTERS then future
  tests get run, otherwise they get filtered out.  A testfilters command
  with no args, resets filtering, so all future tests will get run.
  If TESTFILTERS is not set then the testfilters commands are ignored and
  have no effect.

  This is how one would call the make command if you want to include tests
  which follow the 'testfilters: single' command.

     make  TESTFILTERS=single regression_test_node1_for_QA iniport=\$PGPORT

  You can put the 'TESTFILTERS=...' assignment anywhere on the command line, e.g.:

     make regression_test_node1_for_QA iniport=\$PGPORT TESTFILTERS=single

  Here is a snippet from a dot in file using the testfilters command:

    testfilters:  four  single
    cd: QARegres/MultiColumnJoin
    test: MultiColumnInEquiJoin
    testfilters:

  So if you ran the previous make command, then $me would run the
  'cd:' command and the 'test:' command because the word 'single' was found
  in both the TESTFILTERS variable and in the 'testfilters:' command args.

  Valid testfilters are: four, single, 1dn and 2dn.  The $me script
  examines the values passed to TESTFILTERS and all 'testfilters:' command
  args to make sure they are valid.

The exit status is 0 if all tests passed, 1 if some tests failed, and 2
if the tests could not be run for some reason.

Report bugs to <pgsql-bugs@postgresql.org>."


message(){
    _dashes='==============' # 14
    _spaces='                                                  ' # 50
    _msg=`echo "$1$_spaces" | cut -c 1-50`
    echo "$_dashes $_msg $_dashes"
}

trouble_exit()
{
    #when fatal error occurs, inject messsage into $diff_file so test_checkresults will catch it
    echo "trouble" 1>&2
    echo "======================================================================" >> "$diff_file"
    echo "=== FATAL ERROR in following command:                             ====" >> "$diff_file"
    echo "$2" >> "$diff_file"
    echo "======================================================================" >> "$diff_file"
    (exit $1); exit
}

# ----------
# Unset locale settings
# ----------

unset LC_COLLATE LC_CTYPE LC_MONETARY LC_MESSAGES LC_NUMERIC LC_TIME LC_ALL LANG LANGUAGE

# On Windows the default locale may not be English, so force it
case $host_platform in
    *-*-cygwin*|*-*-mingw32*)
	LANG=en
	export LANG
	;;
esac


# ----------
# Check for echo -n vs echo \c
# ----------

if echo '\c' | grep c >/dev/null 2>&1; then
    ECHO_N='echo -n'
    ECHO_C=''
else
    ECHO_N='echo'
    ECHO_C='\c'
fi


# ----------
# Initialize default settings
# ----------

: ${inputdir=.}
: ${outputdir=.}

libdir='@libdir@'
pkglibdir='@pkglibdir@'
bindir='@bindir@'
datadir='@datadir@'
host_platform='@host_tuple@'
enable_shared='@enable_shared@'
GCC=@GCC@
client=
EXTRADIFFFLAGS=

if [ "$GCC" = yes ]; then
    compiler=gcc
else
    compiler=cc
fi

unset mode
unset schedule
unset debug
unset top_builddir
unset temp_install
unset multibyte
unset unique

unique=`id -u`
if [ -n "$PGDBNAME" ]; then
    dbname="$PGDBNAME"
else
dbname=verticadb"$unique"
fi

hostname=localhost
maxconnections=0

: ${GMAKE='@GMAKE@'}


# ----------
# Parse command line options
# ----------

while [ "$#" -gt 0 ]
do
    case $1 in
        --help|-\?)
                echo "$help"
                exit 0;;
        --version)
                echo "pg_regress (PostgreSQL @VERSION@)"
                exit 0;;
        --debug)
                debug=yes
                shift;;
        --client=*)
                client=`expr "x$1" : "x--client=\(.*\)"`
                shift;;
        --ignore=*)
		ignore=`expr "x$1" : "x--ignore=\(.*\)"`
	        for f in $ignore; do
                    EXTRADIFFFLAGS="$EXTRADIFFFLAGS `cat $inputdir/pg_regress.$f`"
		done
                shift;;
        --ignore-warnings)
                EXTRADIFFFLAGS="$EXTRADIFFFLAGS -I ^WARNING -I execute.warning -I ^empty.query"
                shift;;
        --ignore-errors)
                EXTRADIFFFLAGS="$EXTRADIFFFLAGS -I ^ERROR -I ^LINE.\\d\\d* -I ^\\s*\\^"
                shift;;
        --ignore-hints)
                EXTRADIFFFLAGS="$EXTRADIFFFLAGS -I ^HINT -I ^NOTICE"
                shift;;
        --ignore-psql)
                EXTRADIFFFLAGS="$EXTRADIFFFLAGS -I ^psql: -I ^vsql:"
                shift;;
        --ignore-echo)
                EXTRADIFFFLAGS="$EXTRADIFFFLAGS -I ^\\\\set\\sECHO"
                shift;;
        --inputdir=*)
                inputdir=`expr "x$1" : "x--inputdir=\(.*\)"`
                shift;;
        --outputdir=*)
                outputdir=`expr "x$1" : "x--outputdir=\(.*\)"`
                shift;;
        --schedule=*)
                foo=`expr "x$1" : "x--schedule=\(.*\)"`
                schedule="$schedule $foo"
                shift;;
        --top-builddir=*)
                top_builddir=`expr "x$1" : "x--top-builddir=\(.*\)"`
                shift;;
        --host=*)
                PGHOST=`expr "x$1" : "x--host=\(.*\)"`
                export PGHOST
                unset PGHOSTADDR
                shift;;
        --port=*)
                PGPORT=`expr "x$1" : "x--port=\(.*\)"`
                export PGPORT
                shift;;
        --user=*)
                PGUSER=`expr "x$1" : "x--user=\(.*\)"`
                export PGUSER
                shift;;
        -*)
                echo "$me: invalid argument $1" 1>&2
                exit 2;;
        *)
                extra_tests="$extra_tests $1"
                shift;;
    esac
done

# ----------
# warn of Cygwin likely failure if maxconnections = 0
# and we are running parallel tests
# ----------

case $host_platform in
    *-*-cygwin*)
	case "$schedule" in
	    *parallel_schedule*)
		if [ $maxconnections -eq 0 ] ; then
		    echo Using unlimited parallel connections is likely to fail or hang on Cygwin.
		    echo Try \"$me --max-connections=n\" or \"gmake MAX_CONNECTIONS=n check\"
		    echo with n = 5 or 10 if this happens.
		    echo
		fi
		;;
	esac
	;;
esac


# ----------
# On some platforms we can't use Unix sockets.
# ----------
case $host_platform in
    *-*-cygwin* | *-*-mingw32* | *-*-qnx* | *beos*)
        unix_sockets=no;;
    *)
        unix_sockets=yes;;
esac


# ----------
# Set up diff to ignore horizontal white space differences.
# ----------

case $host_platform in
    *-*-qnx* | *-*-sco3.2v5*)
        DIFFFLAGS='-b';;
    *)
        DIFFFLAGS='-w';;
esac

# diff flags for all uses of diff
# -I patterns to ignore changed lines go in SQLTest/pg_regress.skip
# diff uses grep Basic Regular Expressions, so the r.e. operators
#  ? + { | ( )   need to be backslashed in -I patterns, e.g. '\|'
# You can't quote spaces in pg_regress.skip.
DIFFFLAGS="$DIFFFLAGS $EXTRADIFFFLAGS -I ^Time: `cat $inputdir/test_driver.skip`"

# ----------
# Set backend timezone and datestyle explicitly
#
# To pass the horology test in its current form, the postmaster must be
# started with PGDATESTYLE=ISO, while the frontend must be started with
# PGDATESTYLE=Postgres.  We set the postmaster values here and change
# to the frontend settings after the postmaster has been started.
# ----------

PGTZ='PST8PDT'; export PGTZ
PGDATESTYLE='ISO, MDY'; export PGDATESTYLE


# ----------
# Exit trap to remove temp file and shut down postmaster
# ----------

# Note:  There are some stupid shells (even among recent ones) that
# ignore the argument to exit (as in `exit 1') if there is an exit
# trap.  The trap (and thus the shell script) will then always exit
# with the result of the last shell command before the `exit'.  Hence
# we have to write `(exit x); exit' below this point.

exit_trap(){ 
    savestatus=$1
    if [ -n "$postmaster_pid" ]; then
        kill -2 "$postmaster_pid"
        wait "$postmaster_pid"
        unset postmaster_pid
    fi
    rm -f "$TMPFILE" && exit $savestatus
}

trap 'exit_trap $?' 0

sig_trap() {
    savestatus=$1
    echo; echo "caught signal"
    if [ -n "$postmaster_pid" ]; then
        echo "signalling fast shutdown to postmaster with pid $postmaster_pid"
        kill -2 "$postmaster_pid"
        wait "$postmaster_pid"
        unset postmaster_pid
    fi
    (exit $savestatus); exit
}

trap 'sig_trap $?' 1 2 13 15

LOGDIR=$outputdir/log

# ----------
# Windows needs shared libraries in PATH. (Only those linked into
# executables, not dlopen'ed ones)
# ----------
case $host_platform in
	*-*-cygwin*|*-*-mingw32*)
		PATH=$libdir:$PATH
		export PATH
		;;
esac

if [ -n "$PGPORT" ]; then
	port_info="port $PGPORT"
else
	port_info="default port"
fi

$ECHO_N "`date  '+%b %-d %T'`$schedule$extra_tests $ECHO_C"
if [ -n "$PGHOST" ]; then
	echo "(using postmaster on $PGHOST, $port_info)"
else
	if [ "$unix_sockets" = no ]; then
		echo "(using postmaster on localhost, $port_info)"
	else
		echo "(using postmaster on Unix socket, $port_info)"
	fi
fi

# errors can be ignored


# ----------
# Set up SQL shell for the test.
# ----------

echo $outputdir
PSQL="$client $CLIENT_OPTIONS -l $outputdir/pigshell.out"

echo $PSQL

# ----------
# Set frontend timezone and datestyle explicitly
# ----------

PGTZ='PST8PDT'; export PGTZ
PGDATESTYLE='Postgres, MDY'; export PGDATESTYLE


# ----------
# Let's go
# ----------

message "running regression test queries"

if [ ! -d "$outputdir/results" ]; then
    mkdir -p "$outputdir/results" || { (exit 2); exit; }
fi
result_summary_file=$outputdir/regression.out
diff_file=$outputdir/regression.diffs
timing_file=$outputdir/timing.out

cat /dev/null >"$result_summary_file"
cat /dev/null >"$diff_file"
cat /dev/null >"$timing_file"


SQL_DIR=scripts
WORK_DIR=$inputdir

# Filter tests based upon values found in the TESTFILTERS environment
# variable and the testfilters command args in the schedule (dot in) file.
# If one of the testfilters args match what's in TESTFILTERS then future
# tests get run, otherwise they get filtered out.  A testfilters command
# with no args, resets filtering, i.e. future tests will get run.
# If TESTFILTERS is not set then the testfilters command has no effect.
#
filter_test=false
valid_testfilters="single four 1dn 2dn"
# Validate TESTFILTERS 
for tf in $TESTFILTERS; do
    for vtf in $valid_testfilters; do
        if [ $tf = $vtf ]; then
            continue 2
        fi
    done
    echo "Invalid test filter specified in TESTFILTERS: $tf"
    echo "Valid filters are: $valid_testfilters"
    (exit 2); exit
done

lno=0
(
    [ "$enable_shared" != yes ] && echo "ignore: plpgsql"
    cat $schedule </dev/null
    for x in $extra_tests; do
        echo "test: $x"
    done
) | sed 's/[ 	]*#.*//g' | \
while read line
do
    # Count line numbers
    lno=`expr $lno + 1`
    [ -z "$line" ] && continue

    set X $line; shift
    cmd=$1
    if [ x"$cmd" = x"testfilters:" ]; then
        # No effect if TESTFILTERS is not set
        [ -z "$TESTFILTERS" ] && continue;
        shift
        cur_testfilters="$*"
        if [ -z "$cur_testfilters" ]; then
            # reset filtering
            filter_test=false
            continue
        fi
        # Validate this testfilters args
        for ctf in $cur_testfilters; do
            valid_filter=false
            for vtf in $valid_testfilters; do
                if [ $ctf = $vtf ]; then
                    valid_filter=true
                    break
                fi
            done
            if [ $valid_filter = false ]; then
                trouble_exit 2 "Invalid test filter on line $lno: $ctf.  Valid filters are: $valid_testfilters"
            fi
        done
        for ctf in $cur_testfilters; do
            for tf in $TESTFILTERS; do
                if [ $tf = $ctf ]; then
                    # turn off filtering
                    filter_test=false
                    continue 3
                fi
            done
        done
        filter_test=true
        continue
    fi

    if [ $filter_test = true ]; then
        continue;
    fi

    if [ x"$cmd" = x"test64:" ]; then
        if [ f"`uname -p`" = f"x86_64" ]; then
            cmd="test:"
        else
            continue
        fi
    fi
    # testdebug: run only in debug build
    if [ x"$cmd" = x"testdebug:" ]; then
        if [ x"$BUILD_QUALIFIERS" = x"_Debug" ]; then
            cmd="test:"
        else
            continue
        fi
    fi
    
    # testnovg: do not run for valgrind test
    if [ x"$cmd" = x"testnovg:" ]; then
        if [ -z $RUN_VALGRIND ]; then
            cmd="test:"
        else
            continue
        fi
    fi 
       
    if [ x"$cmd" = x"ignore:" ]; then
        shift
        ignore_list="$ignore_list $@"
        continue
    elif [ x"$cmd" = x"echo:" ]; then
        shift
        echo "$*"
        continue
    elif [ x"$cmd" = x"cd:" ]; then
        shift
        formatted=`echo $1 | awk '{printf "%-64s", $1;}'`
        $ECHO_N "cd   $formatted ... $ECHO_C"
        if [ $# -eq 0 ]; then  # Reset to default env
            SQL_DIR=scripts
            WORK_DIR="$inputdir"
        else
            SQL_DIR=$1
            WORK_DIR="$inputdir/$SQL_DIR"
        fi
        cd $WORK_DIR >/dev/null 2>&1
        if [ $? -eq 0 ]; then
            echo "ok"
            continue
        else
            trouble_exit 2 "cd $WORK_DIR"
        fi
    elif [ x"$cmd" = x"test:" ]; then
        shift
        if [ $# -eq 0 ]; then  
            echo "Missing test name on line $lno" 2>&1
            trouble_exit 2 "Missing test name on line $lno"
        fi
        if [ "`echo $1 | grep /`" ]; then  # test: setup/reg_003_site4_s2_p3b_ALL_ROS_setup
            TEST_DIRNAME=`dirname $1`
            if [ x"$WORK_DIR" = x"$inputdir" ]; then
                EXPECTED_DIR=$TEST_DIRNAME/expected
                RESULTS_DIR=results
                SQL_DIR=""
            else
                EXPECTED_DIR=$SQL_DIR/$TEST_DIRNAME/expected
                RESULTS_DIR=results/$SQL_DIR
            fi
            if [ ! -d "$outputdir/$RESULTS_DIR/$TEST_DIRNAME" ]; then
                mkdir -p "$outputdir/$RESULTS_DIR/$TEST_DIRNAME" || trouble_exit 2 "mkdir -p $outputdir/$RESULTS_DIR/$TEST_DIRNAME"
            fi
        else  # test: reg_003_site4_s2_p3b_ALL_ROS_setup
            if [ x"$WORK_DIR" = x"$inputdir" ]; then
				if [ x"$DISTRO_NAME" != x"" ]; then
					EXPECTED_DIR=$DISTRO_NAME/expected
				else
                	EXPECTED_DIR=expected
				fi
                RESULTS_DIR=results
                SQL_DIR=scripts
            else
                EXPECTED_DIR=$SQL_DIR/expected
                RESULTS_DIR=results/$SQL_DIR
            fi
            if [ ! -d "$outputdir/$RESULTS_DIR" ]; then
                mkdir -p "$outputdir/$RESULTS_DIR" || trouble_exit 2 "mkdir -p $outputdir/$RESULTS_DIR"
            fi
        fi
    else
        echo "$me:$schedule:$lno: syntax error"
        trouble_exit 2 "$me:$schedule:$lno: syntax error"
    fi

    # ----------
    # Start tests
    # ----------

    # Profiling: note when test started
    echo `date +'%s'` \(`date`\) Execution Starting $1 >> $timing_file
    if [ $# -eq 1 ]; then
        # Run a single test
        formatted=`echo $1 | awk '{printf "%-64s", $1;}'`
        $ECHO_N "test $formatted ... $ECHO_C"
        ( $PSQL -f "$inputdir/$SQL_DIR/$1.pig" > "$outputdir/$RESULTS_DIR/$1.out" 2>&1)&
        wait
    else
        # Start a parallel group
        $ECHO_N "parallel group ($# tests): $ECHO_C"
        if [ $maxconnections -gt 0 ] ; then
            connnum=0
            test $# -gt $maxconnections && $ECHO_N "(in groups of $maxconnections) $ECHO_C"
        fi
        for name do
            ( 
              $PSQL -f "$inputdir/$SQL_DIR/$name.pig" > "$outputdir/$RESULTS_DIR/$name.out" 2>&1
              $ECHO_N " $name$ECHO_C"
            ) &
            if [ $maxconnections -gt 0 ] ; then
                connnum=`expr \( $connnum + 1 \) % $maxconnections`
                test $connnum -eq 0 && wait
            fi
        done
        wait
        echo
    fi
    # Profiling: note when execution is done
    echo `date +'%s'` \(`date`\) Execution Done $1 >> $timing_file


    # ----------
    # Run diff
    # (We do not want to run the diffs immediately after each test,
    # because they would certainly get corrupted if run in parallel
    # subshells.)
    # ----------

    # Profiling: note when diff starts
    echo `date +'%s'` \(`date`\) Diff Starting $1 >> $timing_file
    for name do
        if [ $# -ne 1 ]; then
            formatted=`echo "$name" | awk '{printf "%-64s", $1;}'`
            $ECHO_N "     $formatted ... $ECHO_C"
        fi
        if  [ x"$SQL_DIR" = x"sql" ]; then
            EXPECTED="$inputdir/$EXPECTED_DIR/${name}"
        else
            EXPECTED="$inputdir/$EXPECTED_DIR/`basename $name`"
        fi

        # Check list extracted from resultmap to see if we should compare
        # to a system-specific expected file.
        # There shouldn't be multiple matches, but take the last if there are.
        for LINE in $SUBSTLIST
        do
            if [ `expr "$LINE" : "$name="` -ne 0 ]
            then
                SUBST=`echo "$LINE" | sed 's/^.*=//'`
                if  [ x"$SQL_DIR" = x"sql" ]; then
                    EXPECTED="$inputdir/$EXPECTED_DIR/${SUBST}"
                else
                    EXPECTED="$inputdir/$EXPECTED_DIR/`basename ${SUBST}`"
                fi
            fi
        done
        # If there are multiple equally valid result files, loop to get the right one.
        # If none match, diff against the closest one.

        bestfile=
        bestdiff=
	bestresult=2

	# If there is a result set, use it only, otherwise use a single result
	resultlist=`shopt -s nullglob; echo ${EXPECTED}.out[0-9]`
	if [ -z "$resultlist" ]; then
	    resultlist=$EXPECTED.out
	fi

        for thisfile in $resultlist; do
            [ ! -r "$thisfile" ] && continue

	    # -I patterns to ignore changed lines go in SQLTest/pg_regress.skip
	    $DIFF -q $DIFFFLAGS -I '^\\i ' \
		$thisfile $outputdir/$RESULTS_DIR/${name}.out >/dev/null
            result=$?
            case $result in
                0) bestresult=0; break;;
                1|3) thisdiff=`$DIF2 $DIFFFLAGS $thisfile $outputdir/$RESULTS_DIR/${name}.out 2>/dev/null | wc -l`
                   if [ -z "$bestdiff" ] || [ "$thisdiff" -lt "$bestdiff" ]; then
                       bestdiff=$thisdiff; bestfile=$thisfile; bestresult=$result
                   fi
                   continue;;
                2) break;;
            esac
        done


        # Now print the result.

        case $bestresult in
            0)
                echo "ok";;
            1|3)
                ( $DIF2 $DIFFFLAGS $bestfile $outputdir/$RESULTS_DIR/${name}.out
                  echo
                  ##echo "$DIF2 $DIFFFLAGS $bestfile"
                  echo "======================================================================"
                  echo ) >> "$diff_file"
                if echo " $ignore_list " | grep " $name " >/dev/null 2>&1 ; then
                    echo "failed (ignored)"
                elif [ $bestresult -eq 3 ]; then
                    echo "message change"
		else
		    echo "FAILED"
                fi
                ;;
            2)
                # disaster struck
                trouble_exit 2 "$DIF2 $DIFFFLAGS $bestfile $outputdir/$RESULTS_DIR/${name}.out"
        esac
    done
    # Profiling: note when diff starts
    echo `date +'%s'` \(`date`\) Diff Done $1 >> $timing_file

done | tee "$result_summary_file" 2>&1

[ $? -ne 0 ] && exit

# ----------
# Evaluation
# ----------

count_total=`cat "$result_summary_file" | grep '\.\.\.' | wc -l | sed 's/ //g'`
count_ok=`cat "$result_summary_file" | grep '\.\.\. ok' | wc -l | sed 's/ //g'`
count_failed=`cat "$result_summary_file" | grep '\.\.\. FAILED' | wc -l | sed 's/ //g'`
count_ignored=`cat "$result_summary_file" | grep '\.\.\. failed (ignored)' | wc -l | sed 's/ //g'`

echo
if [ $count_total -eq $count_ok ]; then
    msg="All $count_total tests passed."
    result=0
elif [ $count_failed -eq 0 ]; then
    msg="$count_ok of $count_total tests passed, $count_ignored failed test(s) ignored."
    result=0
elif [ $count_ignored -eq 0 ]; then
    msg="$count_failed of $count_total tests failed."
    result=1
else
    msg="`expr $count_failed + $count_ignored` of $count_total tests failed, $count_ignored of these failures ignored."
    result=1
fi

dashes=`echo " $msg " | sed 's/./=/g'`
echo "$dashes"
echo " $msg "
echo "$dashes"
echo

if [ -s "$diff_file" ]; then
    echo "The differences can be viewed like this:"
    echo "  less $diff_file"
    echo "The test summary shown above is saved in \`$result_summary_file'."
    echo
else
    rm -f "$diff_file" "$result_summary_file"
fi


(exit $result); exit

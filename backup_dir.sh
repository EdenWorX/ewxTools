#!/bin/bash

SHORT="a,c,f,h,l:,n,s:t:,v,z"
LONG="auto,cleanup,fsync,help,logfile,no-dir,source:,target:,verify,compress"
OPTS=$(getopt --name backup_dir --options $SHORT --longoptions $LONG -- "$@")

eval set -- "$OPTS"

xAsk="yes"
xCompress=""
xDel=""
xLogFile=""
xShowHelp="no"
xSrc=""
xSubDir="yes"
ySync=""
xTgt=""
xVerify="no"

while :
do
	case "$1" in
		-a | --auto )
			xAsk="no"
			shift 1
			;;
		-c | --cleanup )
			xDel="--delete-during"
			shift 1
			;;
		-h | --help )
			xShowHelp="yes"
			shift 1
			;;
		-f | --fsync )
			xSync="--fsync"
			shift 1
			;;
		-l | --logfile )
			xLogFile="$2"
			shift 2
			;;
		-n | --no-dir )
			xSubDir="no"
			shift 1
			;;
		-s | --source )
			xSrc="$2"
			shift 2
			;;
		-t | --target )
			xTgt="$2"
			shift 2
			;;
		-v | --verify )
			xVerify="yes"
			shift 1
			;;
		-z | --compress )
			xCompress="z"
			shift 1
			;;
		--)
			shift;
			break
			;;
		*)
			echo "Unexpected option \"$1\" encountered"
			exit 1
			;;
	esac
done

xExtras="$@"


if [[ "x$xTgt" = "x" || "x$xSrc" = "x" || "x$xShowHelp" = "xyes" ]] ; then
	echo "Usage: $0 [OPTIONS] <-s|--source source> <-t|--target target> [-- [rsync options]]"
	echo
	echo "Backup <source> into <target>"
	echo "Return 0 on success, 1 if rsync failed, 2 if CTRL-C was caught"
	echo
	echo "OPTIONS:"
	echo "  -a --auto     : Do not ask, start directly. Use with care!"
	echo "  -c --cleanup  : Files in the target that no longer exit in source are deleted"
	echo "  -f --fsync    : Do fsync after each written file."
	echo "  -h --help     : Show this help and exit"
	echo "  -l --logfile  : Set a log file. Default is to write into the parent of the target"
	echo "  -n --no-dir   : Do not create a subdirectory in the target, copy directly"
	echo "  -v --verify   : Add a second rsync run that checks all checksums"
	echo "  -z --compress : Compress file streams. Only use on very slow network connections"
  exit 0
fi

# Normalize trailing spaces
xSrc="$(echo -n "$xSrc" | sed -e 's,/*$,,g')/"
xTgt="$(echo -n "$xTgt" | sed -e 's,/*$,,g')/"

# If the --no-dir option was not used, add basename of source as a subdirectory to target
if [[ "yes" = "$xSubDir" ]]; then
	xTgt="$xTgt$(basename $xSrc)/"
fi


# Log into dirname of (the final) target, that should always be writable by the caller.
xLog="$(dirname $xTgt)/backup_$(basename $xSrc)_$(date '+%Y%m%d').log"

# If a logfile was set, use that instead
if [[ "x" != "x$xLogFile" ]]; then
	xLog="${xLogFile}"
fi


function log {
	local xMsg="$@"
	local xNow="$(date '+%Y-%m-%d %H:%M:%S')"
	echo "$xNow : $xMsg"
	echo "$xNow : $xMsg" >> $xLog
	return 0
}


# Instead of getting killed directly by CTRL-C, let's try a cleaner interrupt.
# Also a verification run should not start if we where told to break off
xHaveCtrlC=0


function ctrlc {
	xHaveCtrlC=1
	log "CTRL-C caught, breaking off..."
	return 0
}


# -----------------------------------------------------------------------------

if [ "x$xAsk" = "xno" ] ; then
	echo -n "Backing up content from $xSrc into $xTgt ("
	if [ -n "$xDel" ] ; then
		echo "WITH cleanup)"
	else
		echo " no  cleanup)"
	fi
else
	echo -n "Backup content from $xSrc into $xTgt ? [Y/n] ("
	if [ -n "$xDel" ] ; then
		echo -n "WITH cleanup) : "
	else
		echo -n " no  cleanup) : "
	fi
	read answer

	if [ "xn" = "x$answer" ] ; then
		echo "Backup aborted"
		exit 0
	fi
fi

xReturn=0

# FIRST run: Normal rsync backup
if [[ 0 -eq $xHaveCtrlC ]]; then
	log "Start Backup $xSrc into $xTgt"
	cmd_args="-avhHA${xCompress} $xDel --log-file=$xLog --progress $xSync $xExtras"
	cmd="rsync $cmd_args --size-only $xSrc $xTgt"
	log "$cmd"
	$cmd
	xReturn=$?
fi

# SECOND run; Verification rsync check
if [[ "x$xVerify" = "xyes" && 0 -eq $xHaveCtrlC && 0 -eq $xReturn ]]; then
	log "Start Verify $xTgt"
	cmd="rsync $cmd_args -c $xSrc $xTgt"
	log "$cmd"
	$cmd
	xReturn=$?
fi

# Make sure we do post a useful exit code
xExit=0
if [[ 0 -ne $xReturn ]];    then xExit=1; fi
if [[ 0 -ne $xHaveCtrlC ]]; then xExit=2; fi

log "End Backup $xSrc into $xTgt [Exit $xExit]"


# Pack logfile to safe space
if [[ -f "${xLog}" ]]; then
	bzip2 "${xLog}"
fi


exit $xExit

#!/bin/bash

CAC=$(which cac)
if [[ "x" = "x${CAC}" ]]; then
	echo "Unable to find cac!"
	exit 42
fi

# Let's make sure this is our cac...
if [[ ! "$(${CAC} --version 2>&1)" =~ 'EWX cac V' ]]; then
	echo "Wrong cac!"
	echo -n "Got: "
	${CAC} --version 2>&1
	echo "Expected: EWX cac"
	exit 43
fi

SHORT="a:,h,p:,s,t:,T:,U"
LONG="archive:,help,prefix:,splitaudio,target:,tempdir:,upgrade"
OPTS=$(getopt --name cac_all --options $SHORT --longoptions $LONG -- "$@")

eval set -- "$OPTS"

xPREFIX=""
xTARGET=""
xARCHIVE=""
xSHOWUSAGE=""
xSPLITAUDIO=""
xDOUPGRADE=""
xTEMPDIR=""
xEXIT=0

xHaveArchive="no"

while :
do
	case "$1" in
		-a | --archive )
			xARCHIVE="$2"
			xHaveArchive="yes"
			shift 2
		;;
		-h | --help )
			xSHOWUSAGE="yes"
			shift 1
		;;
		-p | --prefix )
			xPREFIX="$2"
			shift 2
		;;
		-s | --splitaudio)
			xSPLITAUDIO="--splitaudio"
			shift 1
		;;
		-t | --target )
			xTARGET="$2"
			shift 2
		;;
		-T | --tempdir )
			xTEMPDIR="--tempdir $2"
			shift 2
		;;
		-U | --upgrade )
			xDOUPGRADE="--upgrade"
			shift 1
		;;
		--)
			shift;
			break
			;;
		*)
			echo "Unexpected option \"$1\" encountered"
			xSHOWUSAGE="yes"
			xEXIT=1
			shift 1
			;;
	esac
done


if [[ "x" = "x${xPREFIX}" ]]; then
    echo "ERROR: Prefix was not set!"
    xSHOWUSAGE="yes"
    xEXIT=2
fi


if [[ "x" = "x${xTARGET}" ]]; then
    echo "ERROR: Target was not set!"
    xSHOWUSAGE="yes"
    xEXIT=3
fi


if [[ "xyes" = "x${xSHOWUSAGE}" ]]; then
	echo "Usage: $0 <-h|--help>"
	echo "Usage: $0 [OPTIONS] <-p|--prefix prefix> <-t|--target target> [-a|--archive archive]"
	echo
	echo "[c]leanup [a]nd [c]onvert all <prefix>*.[mkv|mp4|webm] into <target> and"
	echo "move all that succeeded to [archive] if set"
	echo
	echo "Checks <target> for existence of each video and works with lock files to ensure"
	echo "to not produce any double conversions."
	echo
	echo "OPTIONS:"
	echo "  -a --archive <archive> : Videos are moved there after processing"
	echo "  -h --help              : Show this help and exit."
	echo "  -s --splitaudio        : Split the second channel, if it exists, into its own wav"
	echo "  -T --tempdir <path>    : Declare an alternative temporary directory for the processing"
	echo "  -U --upgrade           : Force 60 FPS when 30 FPS would be the target (source < 50 FPS)"
	exit ${xEXIT}
fi

if [[ ! -d "${xTARGET}" ]]; then
	mkdir -p "${xTARGET}"
	res=$?
	if [[ 0 -ne $res ]]; then
		echo "ERROR: mkdir \"${xTARGET}\" failed: $res"
		exit $res
	fi
fi


if [[ "xyes" = "x${xHaveArchive}" && ! -d "${xARCHIVE}" ]]; then
	mkdir -p "${xARCHIVE}"
	res=$?
	if [[ 0 -ne $res ]]; then
		echo "ERROR: mkdir \"${xARCHIVE}\" failed: $res"
		exit $res
	fi
fi

declare -a xInputFiles=()
mapfile -t xInputFiles < <( \
	find ./ -mindepth 1 -maxdepth 1 \( \
		-name "${xPREFIX}*.mkv" -or    \
		-name "${xPREFIX}*.mp4" -or    \
		-name "${xPREFIX}*.mpg" -or    \
		-name "${xPREFIX}*.mpeg" -or   \
		-name "${xPREFIX}*.webm" -or   \
		-name "${xPREFIX}*.avi"        \
	\) -printf "%f\n" 2>/dev/null | sort -h )

for mkv in "${xInputFiles[@]}"; do
	if [[ "x" = "x$mkv" ]]; then
		continue 1
	fi

	dest="${xTARGET}/${mkv%.*}.mkv"
	lock="${xTARGET}/${mkv%.*}.lck"
	hasl=0

	if [[ ! -f "${dest}" ]]; then
		lockfile -r0 "$lock" 1>/dev/null 2>&1
		if [[ 0 -eq $? ]]; then
				hasl=1
		fi
	fi

	if [[ $hasl -eq 1 ]]; then
		${CAC} --input "${mkv}" --output "${dest}" ${xSPLITAUDIO} ${xDOUPGRADE} ${xTEMPDIR}
		res=$?
		rm -f "${lock}"
		
		if [[ 0 -eq $res ]]; then
			if [[ "xyes" = "x${xHaveArchive}" ]]; then
				echo -n "Moving \"$mkv\" ..."
				mv "${mkv}" "${xARCHIVE}/"
				echo " done"
			else
				echo "Not moving \"$mkv\" as no archive was set"
			fi
		elif [[ 42 -eq $res ]]; then
			# User interruption via CTRL+C (SIGINT) or SIGTERM
			echo "User interruption caught. Exiting..."
			exit $res
		elif [[ 255 -ne $res ]]; then
			echo "${CAC} FAILED: $res"
		fi
	else
		echo "${dest} already in progress. Skipping..."
	fi
done

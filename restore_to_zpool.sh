#!/bin/bash

VERSION="1.0"

# Version 0.1, 20240606, sed - First Version

xPool="$1"
xBack="$2"
xNoDo="$3"

if [[ -z "${xBack}" || "-h" = "${xPool}" || "--help" = "${xPool}" ]]; then
	echo " --- $(basename "$0" .sh) Version $VERSION (EdenWorX, sed) ---"
	echo
	echo "Usage: $0 <zpool> <prefix> [--debug]"
	echo
	echo "Check files listed by 'zpool status -v' as defect against a backup, and"
	echo "copy those files back which have a different md5sum or can't be read."
	echo
	echo " zpool    Name of the zpool to check"
	echo " prefix   Prefix of the backup. Only files that can be checked at and copied from"
	echo "          <backup prefix>/file/reported/by/zpool are handled"
	echo " --debug  If added, the script only prints the copy commands it would perform"
	exit 1
fi

[ "--debug" = "${xNoDo}" ] && xCmd="$(which echo)" && xNoDo="${xCmd} \"\\\\n\"; ${xCmd} " || xNoDo=""
echo "zpool : ${xPool}"
echo "prefix: ${xBack}"
[ -n "${xNoDo}" ] && echo "No action, only print!"
echo

cmdCP="$(which cp)"
cmdSudo=""
cmdZPool="$(which zpool)"

if [[ 0 -lt ${UID} ]]; then
	cmdSudo="$(which sudo) "
fi

declare -a xFiles=( )
xHaveStart=0

for line in $(${cmdSudo}${cmdZPool} status -v ${xPool} 2>&1); do
	word="$(echo -n "${line}" | xargs)"
	if [[ 2 -eq ${xHaveStart} ]]; then
		if [[ "/" = "${word:0:1}" ]]; then
			xFiles+=( "${word}" )
		fi
	elif [[ 1 -eq ${xHaveStart} && "files:" = "${word}" ]]; then
		xHaveStart=2
	elif [[ 0 -eq ${xHaveStart} && "following" = "${word}" ]]; then
		xHaveStart=1
	fi
done

for xFile in "${xFiles[@]}"; do
	echo "Checking ${xFile} ..."

	echo -n " => Hash 1: "
	xHashA="$(md5sum -b "${xFile}" 2>/dev/null | cut -d ' ' -f 1 | xargs)"
	[ -z "${xHashA}" ] && xHashA="unavailable"
	echo "${xHashA}"

	echo -n " => Hash 2: "
	xHashB="$(md5sum -b "${xBack}${xFile}" 2>/dev/null | cut -d ' ' -f 1 | xargs)"
	[ -z "${xHashB}" ] && xHashB="unavailable"
	echo "${xHashB}"

	if [[ ( "unavailable" = "${xHashA}" ) || ( "unavailable" != "${xHashB}" && "${xHashA}" != "${xHashB}" ) ]]; then
		echo -n " => restoring..."
		${xNoDo}${cmdSudo}${cmdCP} "${xBack}${xFile}" "$(dirname "${xFile}")/"
		echo "done"
	else
		echo " => file ok"
	fi
done

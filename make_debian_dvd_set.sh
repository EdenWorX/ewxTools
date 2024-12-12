#!/bin/bash

VERSION="1.1"

# Version 1.0, 20240422, sed - First Version
# Version 1.1, 20241212, sed - Make interruptible and control output files

xVers="$1"
xAmnt="$2"
xArchive="https://cdimage.debian.org/cdimage/archive"
xRelease="https://cdimage.debian.org/cdimage/release"
xSubURI="${xVers}/amd64/jigdo-dvd"
if [[ -z $xVers || "-h" = "$xVers" || "--help" = "$xVers" ]]; then
	echo "Usage: $0 <debian version> [amount]"
	echo
	echo " Will create and utilize the debian-<version> directory."
	echo " If the directory/Images exist, jigdo will try to update them."
	echo
	echo "Option [amount]: Only the first six DVDs will be created by"
	echo "                 default. Add the desired amount if you need"
	echo "                 less or wish to create more."
	echo
	echo "Please see:"
	echo "  ${xRelease}"
	echo "for current Debian versions, and"
	echo "  ${xArchive}"
	echo "for previous Debian versions."
	echo -e "\n\tV${VERSION} - sed"
	exit 0
fi

xArchURI="${xArchive}/${xSubURI}"
xRelURI="${xRelease}/${xSubURI}"

# === This must be used strictly non-root ===
if [[ 0 -eq $UID || "root" = "$(whoami)" ]]; then
	echo "Do _NOT_ use this script as root or via sudo!"
	exit 42
fi

# === Add date and time to printed message ===
function say {
	ts=$(date "+%Y.%m.%d %H:%M:%S")
	echo "$ts : $*"
	return 0
}

# === Simple CTRL+C catch trap ===
xWeShallDie=0
function sig_and_die {
	# shellcheck disable=SC2317
	say "Trapped signal, terminating..."
	# shellcheck disable=SC2317
	xWeShallDie=1
	# shellcheck disable=SC2317
	return 0
}

# === Die with a messages ===
function die {
	say "$*"
	exit 2
}

# === Do a popd with an error message on failure ===
function popdir {
	popd || say "popd error: $?"
	return 0
}

# === Little helper to flee if we are commanded to ===
function shall_we_die {
	if [[ 1 -eq ${xWeShallDie} ]]; then
		popdir
		exit 3
	fi
	return 0
}

# Set the signal handler
trap sig_and_die SIGINT SIGQUIT SIGTERM

# pre) default to 6 if no amount was given
if [[ -z ${xAmnt} ]]; then
	xAmnt=6
fi

# 1) Make and cd into new directory
mkdir -p "debian-$xVers" || die "mkdir \"debian-$xVers\" failed: $?"
pushd "debian-$xVers" || die "pushd \"debian-$xVers\" failed: $?"

# 2) Get templates:
for x in jigdo template; do
	shall_we_die
	say "Getting ${x} templates ..."
	for n in $(seq 1 6); do
		shall_we_die
		xTgt="debian-${xVers}-amd64-DVD-${n}.${x}"

		# Now try to fetch the file
		echo -e "\n\tGetting ${x} ${n} => ${xTgt}"
		if wget -q --spider "${xArchURI}/${xTgt}"; then
			shall_we_die
			wget -O "${xTgt}" "${xArchURI}/${xTgt}"
			res=$?
		elif wget -q --spider "${xRelURI}/${xTgt}"; then
			shall_we_die
			wget -O "${xTgt}" "${xRelURI}/${xTgt}"
			res=$?
		else
			shall_we_die
			say "ERROR: Version ${xVers} neither exists in"
			say "  ${xRelease}"
			say "nor in"
			say "  ${xArchive}"
			res=42
		fi
		if [[ 0 -ne $res ]]; then
			say "wget failed: $res"
			popdir
			exit $res
		fi
	done
done

# 2) Make ISOs
for j in debian-*.jigdo; do
	shall_we_die
	say "Building ${j} ..."
	jigdo-lite --noask "${j}"
done

# done
popdir
say "All done"
exit 0

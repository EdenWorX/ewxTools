# EdenWorX Tools

A completely unordered, ungrouped and inconveniently chaotic set of scripts
and programs for random things.


## Disclaimer

All tools are provided as-is and without any promise and/or warranty.

If you break your stuff with any of my tools, you have been warned.

All complains and issues are to be reported at
[this projects github issues page](https://github.com/EdenWorX/ewxTools/issues).

If you like any of my tools, find them useful, would like me to enhance them
for you or if you just have the wish to say thank you, you can always
[buy me a coffee](https://www.buymeacoffee.com/EdenWorX) to do so.


## Standard License Header

Copyright (C) 2024 Sven Eden, EdenWorX

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program. If not, see https://www.gnu.org/licenses/.


## Content

* backup_dir.sh 
  
    Backup directories using rsync with some extras.
* cac[.pl]

    **C**leanup **a**nd **C**onvert videos to ultra high quality h264 with
    focus on usability in video editors and with frames mixed using hardware
    accelerated libplacebo in four parallel portions. Fixed on nvenc atm.
* cac_all.sh

    Little helper script to run all videos in a folder through cac.
* restore_to_zpool.sh

    Simple script to recover files which have been reported as broken by
    `zpool status -v` from a backup. Checks md5 sums first.
* spacefader

    Tool to overwrite free space on devices you can not just dd over.


## backup_dir.sh

This is just a little script to make using rsync for backup purposes, including
the deletion of files which have been deleted in the souce, more convenient.

The performed backup can be checked using checksums if needed.

Current Help Text:
````
Usage: ./backup_dir.sh [OPTIONS] <-s|--source source> <-t|--target target> [-- [rsync options]]

Backup <source> into <target>
Return 0 on success, 1 if rsync failed, 2 if CTRL-C was caught

OPTIONS:
  -a --auto     : Do not ask, start directly. Use with care!
  -c --cleanup  : Files in the target that no longer exit in source are deleted
  -f --fsync    : Do fsync after each written file.
  -h --help     : Show this help and exit
  -l --logfile  : Set a log file. Default is to write into the parent of the target
  -n --no-dir   : Do not create a subdirectory in the target, copy directly
  -v --verify   : Add a second rsync run that checks all checksums
  -z --compress : Compress file streams. Only use on very slow network connections
````


## cac[.pl]

*Note: `cac` is just a symlink on `cac.pl`.*

I do record game videos for my YouTube channel
[@HurryKane76](https://www.youtube.com/@HurryKane76) which I then edit using
the incredible [ShotCut Software](https://www.shotcut.org/).

The recording is done using [OBS Studio](https://obsproject.com) with my own
custom ffmpeg settings to record with near lossless quality.
Those recordings are done in 144 frames per second, but I need them, I-Frames
only, in 60 FPS for the editing.

My hardware is also not the best, so those 144 frames are not perfectly laid
out on the timeline.
Therefore, what was a smooth gaming experience, became quite choppy and chunky
after the scaling down to 60 fps.

What I previously did was to use ffmpeg's mpdecimate filter to erase all frames
which got duplicated due to rendering lag. After that the minterpolate filter
was used to fill in the gaps caused by mpdecimate or by encoding lag.

The result was a whopping performance of 0.4 fps, or about 2.5 hours of
processing for each minute I recorded.
And that long waiting time were completed by a result, where frames not being
exactly on the timeline still caused some choppiness.

Another solution had to be found!
And `cac` is that solution. Instead of doing the mpdecimat+minterpolate
filertering twice and the re-encoding with libx264 in one go, it splits things
up and also parallelizes some of the heftier tasks.
The biggest enhancement is surely the switch from minterpolate, which made
encoding with nvenc impossible, to the frame mixer in libplacebo, which can
work hardware accelerated.
Additionally the following workflow has been implemented:

1) Split the (group of) video(s) into 4 parts
2) Use mpdecimate plus libplacebo frame mixer to clean/mix up to 144(*) fps UTVideo.

    This ensures that the next step is done on a squeaky clean 144(*) fps source.
3) Use mpdecimate plus frame mixer again to scale down to 60 fps, still UTVideo.

    This causes frames to be mixed to match their exact place on the timeline.
4) Re-encode with h264_nvenc, maximum quality, and do the sound remixing.

The sound remixing is another part, that is best done in the last step.
If the first audio stream is in surround sound, it will be remixed to six
channels in 5.1 layout. I record in 7.1 layout, but the developers of ShotCut
have not yet added a 7.1 mode, so I am stuck with remixing down to 5.1.
Additionally, the stream is copied into a second stream, which is remixed to
two channels in stereo mode.
If there is a second audio stream, which would be my voice-under, it can 
optionally be split into an extra file for later re-adding. I really do not
like my voice, so I take the liberty to "enhance" those recordings using the
marvelous [Audacity](https://www.audacityteam.org/) software.

````
NAME
    Cleanup And Convert - cac

USAGE
    cac [-h|OPTIONS] <-i INPUT [-i INPUT2...]> <-o OUTPUT>

ARGUMENTS
    -i | --input
            Path to the input file. Can appear more than once, resulting in
            the output file to be the combination of the input files in
            their given order.

    -o | --output
            The file to write. Must not equal any input file. Must have .mkv
            ending.

OPTIONS
    -h | --help
            This help message

    -t | --tempdir
            Path to the directory where the temporary files are written.
            Defaults to the directory of the input file(s). Ensure to have
            80x the space of the input!

    -s | --splitaudio
            If set, split a second channel (if found) out into a separate
            .wav file. That channel is normally live commentary, and will be
            discarded without this option.

    -u | --upgrade
            Force a target of 60 FPS, even if the source is under 50 FPS.

    -V | --version
            Print version and exit.

DESCRIPTION
    Cleanup And Convert: HurryKane's tool for overhauling gaming clips. (
    See: @HurryKane76 yt channel )

    The program uses ffmpeg to remove duplicate frames and to interpolate
    the video to twice (*) the target FPS in a first step, then do another
    search for duplicate frames and interpolate down the the target FPS.

    If the source has at least 50 FPS in average, the target is set to 60
    FPS. For sources with less than 50 FPS in average, the target is set to
    30 FPS. You can use the -u/--upgrade option to force the target to be 60
    FPS, no matter the source average.

    (*): If the source video has more than twice the target FPS, it will not
    be down- scaled, but the source fps will be kept for the first
    interpolation.
````


## cac_all.sh

When called from within a folder with video files, all files that fit a given
prefix will be run through `cac.pl`. There are not many optins, I use it to
quickly upgrade groups of videos for specific game sessions.

Once the upgrading is complete, the completed video files can optionaly moved
to an archive location. If anything goes wrong, the file stays in place.

The script tries to lock a lockfile first, so you can run it in parallel on the
same video folder. However, since `cac` itself does certain parallelization, it
is no longer recommended to start more than one instance of `cac_all.sh`.

````
Usage: ./cac_all.sh <-h|--help>
Usage: ./cac_all.sh [OPTIONS] <-p|--prefix prefix> <-t|--target target> [-a|--archive archive]

[c]leanup [a]nd [c]onvert all <prefix>*.[avi|mkv|mp4|mpg|mpeg|webm] into <target> and
move all that succeeded to [archive] if set

Checks <target> for existence of each video and works with lock files to ensure
to not produce any double conversions.

OPTIONS:
  -a --archive <archive> : Videos are moved there after processing
  -h --help              : Show this help and exit.
  -s --splitaudio        : Split the second channel, if it exists, into its own wav
  -T --tempdir <path>    : Declare an alternative temporary directory for the processing
  -U --upgrade           : Force 60 FPS when 30 FPS would be the target (source < 50 FPS)
````


## restore_to_zpool.sh

Whenever `zpool status -v` show broken files, I'd like to check each of them
and restore the truly broken ones from my backup drive.

To do this I generate the md5 sums of both files, the seemingly broken one and
its backup. If either the original can not be hashed, or the two hashes differ,
the backup is then copied over the original.

This script simply automates it. Please be aware, that it is a rather crude
tool, and that you should do your own checks first. The script only automates
the "copy what has to be copied" step.

````
 --- restore_to_zpool Version 1.0 (EdenWorX, sed) ---

Usage: ./restore_to_zpool.sh <zpool> <prefix> [--debug]

Check files listed by 'zpool status -v' as defect against a backup, and
copy those files back which have a different md5sum or can't be read.

 zpool    Name of the zpool to check
 prefix   Prefix of the backup. Only files that can be checked at and copied from
          <backup prefix>/file/reported/by/zpool are handled
 --debug  If added, the script only prints the copy commands it would perform
````


## spacefader

This is a very simple tool that fills up free space on any mounted device until
the device is full, and deletes those dummy files thereafter.

If you have deleted a bunch of files from a device you can not just `dd` over
via its blockdevice, `spacefader` ensures that those files are gone forever.

````
  Overwrite all remaining space 1.0.5
------------------------------------------
                   Jan 2010, EdenWorX, sed

This program will fill up the target directory with files of 1 MiB
size with zeros (from /dev/zero) until all the remaining space is
filled up. After all space is wasted, the files are deleted and the
space freed again.

Usage: ./spacefader targetdir [random]

Arguments:
  targetdir - directory in which files are created
              if the directory does not exist it will be created,
              and deleted again after the work is done.
Options:
  random    - fill files from /dev/(u)random instead /dev/zero
````


## Support

Everything should be going through this projects
[GitHub Page](https://github.com/EdenWorX/ewxTools), thank you very much!

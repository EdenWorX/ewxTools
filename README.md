# EdenWorX Tools

A completely unordered, ungrouped and inconveniently chaotic group of scripts
and programs for many random things.


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

*   backup_dir.sh 
  
    Backup directories using rsync with some extras.
* cac[.pl]

    **C**leanup **a**nd **C**onvert videos to ultra high quality h264 with
    focus on usability in video editors and with frames mixed using hardware
    accelerated libplacebo in four parallel portions. Fixed on nvenc atm.
* cac_all.sh

    Little helper script to run all videos in a folder through cac.


### backup_dir.sh

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


### cac[.pl]

*Note: `cac` is just a symlink on `cac.pl`.*

I do record game videos for my YouTube channel
[@HurryKane76](https://www.youtube.com/@HurryKane76) which I then edit using
the incredible [ShotCut Software](https://www.shotcut.org/).

The recording is done using [OBS Studio](https://obsproject.com) with my own
custom ffmpeg settings to record with near lossless quality.
Those recordings are done in 120 frames per second, but I need them, I-Frames
only, in 60 FPS for the editing.

My hardware is also not the best, so those 120 frames are not perfectly laid
out on the timeline.
Therefore, what was a smooth gaming experience, became quite choppy and chunky
after the skaling down to 60 fps.

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
2) Use mpdecimate plus libplacebo frame mixer to clean/mix up to 120 fps UTVideo.

    This ensures that the next step is done on a squeaky clean 120 fps source.
3) Use mpdecimate plus frame mixer again to scale down to 60 fps, still UTVideo.

    This causes frames to be mixed to nmatch their exact place on the timeline.
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
Usage:
    cac [options] <-i INPUT [-i INPUT2...]> <-o OUTPUT>

     Parameters:
            -i | --input        Path to the input file. Can appear more than once, resulting in the output
                                  file to be the combination of the input files in their given order.
            -o | --output       The file to write. Must not equal any input file. Must have .mkv ending.

     Options:
            -h | --help         This help message
            -t | --tempdir      Path to the directory where the temporary files are written. Defaults to the
                                  directory of the input file(s). Ensure to have 50x the space of the input!
            -s | --splitaudio  If set, split a second channel (if found) out into a separate .wav file.
                                 That channel is normally live commentary, and will be discarded without
                                 this option.
            -u | --upgrade     Force a target of 60 FPS, even if the source is under 50 FPS.
            -V | --version     Print version and exit.

     Debug Mode
            -D | --debug       Displays extra information on all the steps of the way.
                                 IMPORTANT: _ALL_ temporary files are kept! Use with caution!
````


## Support

Everything should be going through this projects
[GitHub Page](https://github.com/EdenWorX/ewxTools), thank you very much!
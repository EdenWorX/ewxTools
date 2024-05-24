#!/usr/bin/perl -w
use strict;
use warnings FATAL => 'all';

use threads (
	'yield',
	'stack_size' => 64 * 4096,
	'stringify'
);
use threads::shared;

use Data::Dumper;
use File::Basename;
use Filesys::Df;
use Getopt::Long;
use IPC::System::Simple qw( capturex runx systemx $EXITVAL );
use Pod::Usage;
use Readonly;
use Time::HiRes qw( usleep );


# ===============
# === HISTORY ===
# ===============
# Version  Date        Maintainer     Changes
# 1.0.0    2024-05-23  sed, EdenWorX  First fully working version of the per variant. The Bash variant is dead now.
#
# Please keep this current:
Readonly my $VERSION => "1.0.0";

# =======================================================================================
# Workflow:
# Phase 1: Get Values via ffprobe and determine minimum seconds to split into 5 segments.
# Phase 2: Split the source into 4 segments, streamcopy, length from Phase 1.
# Phase 3: 1 Thread per Segment does mpdecimate(7)+libplacebo(120|60) into UTVideo.
# Phase 4: 1 Thread per Segment does mpdecimate(2)+libplacebo(60|30) into UTVideo.
# Phase 5: h264_nvenc produces output from all segments, highest quality
# Cleanup: segments and temporaries are to be deleted.
# =======================================================================================


# ---------------------------------------------------------
# Shared Variables
# ---------------------------------------------------------
our ( $death_note, $do_debug, $have_progress_msg, $logfile, $ret_global ) :shared;
our @work_status :shared;
our ( $THR_INACTIVE, $THR_CREATED, $THR_RUNNING, $THR_FINISHED, $THR_STOPPED, $THR_JOINED ) :shared;


# ---------------------------------------------------------
# Worker Threads
# ---------------------------------------------------------
sub worker {
	my ( $tid, @cmd ) = @_;
	my $result        = 0;

	# Mark this thread as started
	$work_status[$tid] = $THR_CREATED;

	# Wait for the signal to run the command
	while ( $THR_CREATED == $work_status[$tid] ) {
		yield();
		usleep( 250000 ); ## We check max 4 times a second.
		yield();
	}

	# If the thread was not set to active mode leave now
	if ( $THR_RUNNING != $work_status[$tid] ) {
		$work_status[$tid] = $THR_FINISHED;
		return $result;
	}

	# Run the command
	eval {
		runx( @cmd );
	};

	if ( defined( $@ ) && ( 0 < length( $@ ) ) ) {
		log_error( "Thread failed:\n%s", $@ );
		$result = $EXITVAL;
	}

	# Mark this thread as finished
	$work_status[$tid] = $THR_FINISHED;

	return $result;
}


# ---------------------------------------------------------
# Logging facilities
# ---------------------------------------------------------
our Readonly $LOG_DEBUG;
our Readonly $LOG_STATUS;
our Readonly $LOG_INFO;
our Readonly $LOG_WARNING;
our Readonly $LOG_ERROR;


# ---------------------------------------------------------
# Signal Handlers
# ---------------------------------------------------------
local $SIG{'INT'}  = \&sigHandler;
local $SIG{'QUIT'} = \&sigHandler;
local $SIG{'TERM'} = \&sigHandler;

# Warnings should be logged, too:
local $SIG{__WARN__} = \&warnHandler;

# And fatal errors go to the log as well
local $SIG{__DIE__} = \&dieHandler;


# ---------------------------------------------------------
# Global Constants
# ---------------------------------------------------------
Readonly my $B_FPS => '[fps];[fps]';
Readonly my $B_decimate => '[decim];[decim]';
Readonly my $B_in => '[in]';
Readonly my $B_interp => '[interp];[interp]';
Readonly my $B_middle => '[middle];[middle]';
Readonly my $B_out => '[out]';


# ---------------------------------------------------------
# Global variables
# ---------------------------------------------------------
our $FF;
our $FP;
my @FF_ARGS_ACOPY_FIL  = qw( -map 0 -codec:a copy -vf );
my @FF_ARGS_CODEC_h264 = qw( -codec:v     h264_nvenc -preset:v p7     -tune:v     hq -profile:v   high444p -level:v   5.2
                             -rc:v        vbr        -rgb_mode yuv444 -cq         4  -qmin        1        -qmax      16
                             -temporal_aq 1          -b_adapt  0      -b_ref_mode 0  -zerolatency 1        -multipass 2
                             -forced-idr  1 );
my @FF_ARGS_CODEC_UTV  = qw( -codec:v utvideo -pred median );
my @FF_ARGS_FILTER     = qw( -ignore_unknown -vf );
my @FF_ARGS_FORMAT     = qw( -colorspace bt709 -color_range pc -pix_fmt yuv444p -f matroska -write_crc32 0 );
my @FF_ARGS_INPUT_CUDA = qw( -loglevel level+warning -nostats -init_hw_device cuda:0 -colorspace bt709 -color_range pc -f concat -safe 0 -i );
my @FF_ARGS_INPUT_VULK = qw( -loglevel level+warning -nostats -init_hw_device vulkan:0 -colorspace bt709 -color_range pc -i );
my @FF_ARGS_START      = qw( -hide_banner -loglevel level+info -y );
my @FF_CONCAT_BEGIN    = qw( -loglevel level+warning -nostats -f concat -safe 0 -i );
my @FF_CONCAT_END      = qw( -map 0 -c copy );
my @FP_ARGS            = qw( -hide_banner -loglevel error -v quiet -show_format -of flat=s=_ -show_entries );
my %dir_stats          = (); ## <dir> => { has_space => <what df says>, need_space => wll inputs in there x 50, srcs => @src }
my $do_print_help      = 0;
my $do_print_version   = 0;
my $do_split_audio     = 0;
my $force_upgrade      = 0;
my $max_fps            = 0;
my $target_fps         = 0;
my @path_source        = ();
my $path_target        = "";
my $path_temp          = "";
my $source_count       = 0;
my %source_groups      = ();
my %source_ids         = ();
my %source_info        = ();
my $tmp_pid            = $$;
my $video_stream       = 0;
my $audio_stream       = 0;
my $voice_stream       = -1;
my $audio_channels     = 0;
my $audio_layout       = "guess";
my $voice_channels     = 0;
my $voice_layout       = "guess";
my $work_done          = 0;


# ---------------------------------------------------------
# BEGIN Handler
# ---------------------------------------------------------
BEGIN {
	# constants
	$LOG_DEBUG   = 0;
	$LOG_INFO    = 1;
	$LOG_STATUS  = 2;
	$LOG_WARNING = 3;
	$LOG_ERROR   = 4;

	# thread status
	$THR_INACTIVE = 0;
	$THR_CREATED  = 1;
	$THR_RUNNING  = 2;
	$THR_FINISHED = 3;
	$THR_STOPPED  = 4;
	$THR_JOINED   = 5;

	# signal handling
	$death_note  = 0;
	@work_status = ( $THR_INACTIVE, $THR_INACTIVE, $THR_INACTIVE, $THR_INACTIVE );

	# logging
	$do_debug          = 0;
	$have_progress_msg = 0;
	$logfile           = "";

	# Global return value, is set to 1 by log_error()
	$ret_global = 0;

	# ffmpeg default values
	chomp( $FF = capturex( "which", "ffmpeg" ));
	chomp( $FP = capturex( "which", "ffprobe" ));
} ## End BEGIN


# ---------------------------------------------------------
# Argument handling
# ---------------------------------------------------------
my %program_options = (
	'help|h'        => \$do_print_help,
	'debug|D'       => \$do_debug,
	'input|i=s'     => \@path_source,
	'output|o=s'    => \$path_target,
	'splitaudio|s!' => \$do_split_audio,
	'tempdir|t:s'   => \$path_temp,
	'upgrade|u'     => \$force_upgrade,
	'version|V'     => \$do_print_version
);
GetOptions( %program_options ) or pod2usage( 2 );
$do_print_help > 0 and pod2usage( 0 );
$do_print_version > 0 and print "EWX cac V$VERSION\n" and exit 0;


# ---------------------------------------------------------
# Check Arguments
# ---------------------------------------------------------
check_arguments() > 0 and pod2usage( 1 ); ## The sub has already logged

defined( $FF ) and ( 0 < length( $FF ) ) and -x $FF or log_error( "No ffmpeg available (FF: '%s')", $FF // "undef" ) and exit 3;
defined( $FP ) and ( 0 < length( $FP ) ) and -x $FP or log_error( "No ffprobe available (FP: '%s')", $FP // "undef" ) and exit 3;


# ---------------------------------------------------------
# ================	  MAIN  PROGRAM	  ================
# ---------------------------------------------------------
$work_done = 1; # From now on we consider this program as at work
log_info( "Processing %s start", $path_target );


# ---
# --- 1) we need information about each source file
# ---
if ( can_work() ) {
	analyze_inputs() or exit 6;
}


# ---
# --- 2) All input files per temp directory have to be grouped. Each group is then segmented
# ---    Into four parts, so that four threads can do the interpolation in parallel per group.
# ---
if ( can_work() ) {
	( $source_count > 0 ) and build_source_groups() or ( 1 == $source_count ) and declare_single_source() or exit 7;
}

if ( can_work() ) {
	log_info( "Segmenting source groups..." );
	foreach my $groupID ( sort { $a <=> $b } keys %source_groups ) {
		can_work() or last;
		my $prgfile = sprintf( "%s/temp_%d_progress_%d.log", $source_groups{$groupID}{dir}, $tmp_pid, $groupID );
		segment_source_group( $groupID, $prgfile ) or exit 8;
		-f $prgfile and ( 0 == $do_debug ) and unlink( $prgfile );
	}
}

# Make sure we have a sane target FPS. max_fps is reused as upper fps
if ( can_work() ) {
	$target_fps = ( ( $max_fps < 50 ) && ( 0 == $force_upgrade ) ) ? 30 : 60;
	$max_fps    = 2 * $target_fps;
	log_info( "Decimate and interpolate up to %d FPS", $max_fps );
	log_info( "Then interpolate to the target %d FPS", $target_fps );
}


# ---
# --- 3) Now each groups segments can be decimated and interpolated up to max fps (round 1)
# ---
if ( can_work() ) {
	log_info( "Interpolating segments up to %d FPS...", $max_fps );
	foreach my $groupID ( sort { $a <=> $b } keys %source_groups ) {
		can_work() or last;
		interpolate_source_group( $groupID, "tmp", "iup", 7, 0.5, $max_fps, "pad=ceil(iw/2)*2:ceil(ih/2)*2," ) or exit 9;
	}
}


# ---
# --- 4) Then all groups segments have to be decimated and interpolated down to target fps (round 2)
# ---
if ( can_work() ) {
	log_info( "Interpolating segments down to %d FPS...", $target_fps );
	foreach my $groupID ( sort { $a <=> $b } keys %source_groups ) {
		can_work() or last;
		interpolate_source_group( $groupID, "iup", "idn", 3, 0.667, $target_fps ) or exit 10;
	}
}


# ---
# --- 5) And finally we can put all the latest temp files together and create the target vid
# ---
if ( can_work() ) {
	log_info( "Creating %s ...", $path_target );

	my $lstfile = sprintf( "%s/temp_%d_src.lst", dirname( $path_target ), $tmp_pid );
	my $prgfile = sprintf( "%s/temp_%d_prg.log", dirname( $path_target ), $tmp_pid );
	my $mapfile = $path_target;
	$mapfile =~ s/\.mkv$/.wav/;

	if ( open( my $fOut, ">", $lstfile ) ) {
		foreach my $groupID ( sort { $a <=> $b } keys %source_groups ) {
			for my $i ( 0 .. 3 ) {
				printf( $fOut "file '%s'\n", sprintf( $source_groups{$groupID}{idn}, $i ));
			}
		}
		close( $fOut );
	} else {
		log_error( "Unable to write into '%s': %s", $lstfile, $! );
		exit 11;
	}

	# Having a list file we can go and create our output:
	if ( can_work() ) {
		create_target_file( $lstfile, $prgfile, $mapfile ) or exit 11;
	}

	# When everything is good, we no longer need the list file, progress file and the temp files
	if ( 0 == $do_debug ) {
		-f $lstfile and unlink( $lstfile );
		-f $prgfile and unlink( $prgfile );
		foreach my $groupID ( sort { $a <=> $b } keys %source_groups ) {
			for my $i ( 0 .. 3 ) {
				my $tmpfile = sprintf( $source_groups{$groupID}{idn}, $i );
				log_debug( "Removing %s...", $tmpfile );
				-f $tmpfile and unlink( $tmpfile );
			}
		}
	}
}


# ---------------------------------------------------------
# END Handler
# ---------------------------------------------------------
END {
	# Let's clean up and remove all temporary files, if this is "release" mode,
	# or at least list all "orphaned" files if this is debug mode
	if ( $work_done > 0 ) {
		foreach my $gid ( sort { $a <=> $b } keys %source_groups ) {
			if ( -f $source_groups{$gid}{lst} ) {
				( $do_debug > 0 ) and log_debug( "See: %s", $source_groups{$gid}{lst} ) or unlink( $source_groups{$gid}{lst} );
			}
			for my $area ( "tmp", "idn", "iup", "prg" ) {
				for my $i ( 0 .. 3 ) {
					my $f = sprintf( $source_groups{$gid}{$area}, $i );
					if ( -f $f ) {
						( $do_debug > 0 ) and log_debug( "See: %s", $f ) or unlink( $f );
					}
				}
			}
		}

		( $ret_global > 0 ) and log_error( "Processing %s FAILED!", $path_target ) or log_info( "Processing %s finished", $path_target );

		if ( ( 0 < length( $logfile ) ) && ( -f $logfile ) ) {
			( ( $ret_global > 0 ) || ( 1 == $do_debug ) ) and printf( "\nSee %s for details\n", $logfile ) or unlink( $logfile );
		}
	}
} ## End END


# ---------------------------------------------------------
# ================ FUNCTION IMPLEMENTATION ================
# ---------------------------------------------------------

sub analyze_inputs {
	my $MaxProbeSize = 256 * 1024 * 1024; # Max out probe size at 256 MB, all relevant stream info should be available from that size
	my $MaxProbeDura = 30 * 1000 * 1000;  # Max out analyze duration at 30 seconds. This should be long enough for everything
	my $MaxProbeFPS  = 8 * 120;           # FPS probing is maxed at 8 seconds for 120 FPS recordings.
	my $pathID       = 0;

	foreach my $src ( @path_source ) {
		can_work() or last;

		my $have_video = 0;
		my $have_audio = 0;
		my $have_voice = 0;
		my $inSize     = -s $src;

		$source_info{$src} = {
			dir       => dirname( $src ),
			id        => ++$pathID,
			probeSize => $inSize > $MaxProbeSize ? $MaxProbeSize : $inSize
		};
		$source_ids{$pathID} = $src;

		# Get basic duration
		my $stream_fields = "avg_frame_rate,duration";
		my @fpcmd         = ( $FP, @FP_ARGS, "stream=$stream_fields", "-probesize", $source_info{$src}{probeSize}, $src );
		log_debug( "Calling: %s", join( " ", @fpcmd ));
		my @fplines        = capturex( @fpcmd );
		my $avg_frame_rate = 0;
		my $duration       = 0;
		my $probeDura      = 0;
		my $probeFPS       = 0;
		foreach my $line ( @fplines ) {
			chomp $line;
			( $line =~ m/format_duration="(\d+\.\d+)"/ )
			and $duration = floor( 1. + ( 1. * $1 ));
			( $line =~ m/streams_stream_\d_avg_frame_rate="(\d+)\/(\d+)"/ ) and ( 1. * $1 > 0. ) and ( 1. * $2 > 0. )
			and log_debug( "avg_frame_rate: '%s' => %d / %d", $line, $1, $2 )
			and $avg_frame_rate = floor( 1. * ( ( 1. * $1 ) / ( 1. * $2 ) ));
		}
		( $duration > 0 ) or log_error( "Unable to determine duration of '%s'", $src ) and return 0;
		( $avg_frame_rate > 0 ) or log_error( "Unable to determine average frame rate of '%s'", $src ) and return 0;
		$probeDura = $duration * 1000 * 1000; ## Probe Duration is set up in microseconds.
		$probeFPS  = $avg_frame_rate * 8;
		log_debug( "Duration   : %d", $duration );
		log_debug( "Average FPS: %d", $avg_frame_rate );
		$source_info{$src}{sourceFPS}      = $avg_frame_rate;
		$source_info{$src}{probedDuration} = $probeDura > $MaxProbeDura ? $MaxProbeDura : $probeDura;
		$source_info{$src}{duration}       = $duration;
		$source_info{$src}{probeFPS}       = $probeFPS > $MaxProbeFPS ? $MaxProbeFPS : $probeFPS;
		$source_info{$src}{probeStrings}   = sprintf( "-probesize %d -analyzeduration %d -fpsprobesize %d",
		                                              $source_info{$src}{probeSize}, $source_info{$src}{probedDuration}, $source_info{$src}{probeFPS} );

		# Now that we have good (and sane) values for probing sizes and durations, lets query ffprobe again to get the final value we need.
		can_work() or last;
		$stream_fields = "avg_frame_rate,channels,codec_name,codec_type,nb_streams,pix_fmt,r_frame_rate,stream_type,duration";
		@fpcmd         = ( $FP, @FP_ARGS, "stream=$stream_fields", split( / /, $source_info{$src}{probeStrings} ), $src );
		log_debug( "Calling: %s", join( " ", @fpcmd ));
		@fplines = capturex( @fpcmd );
		foreach my $line ( @fplines ) {
			chomp $line;
			#log_debug("RAW[%s]", $line);
			( $line =~ m/streams_stream_(\d)_([^=]+)="?([^"]+)"?/ ) and $source_info{$src}{streams}[$1]{$2} = "$3"
			and log_debug( "Stream %d: %s = '%s'", $1, $2, $3 ) and next;
			( $line =~ m/format_([^=]+)="?([^"]+)"?/ ) and $source_info{$src}{format}{$1} = "$2"
			and log_debug( "Format  : %s = '%s'", $1, $2 );
		}

		# Maybe we have to fix the duration values we currently have
		$avg_frame_rate                                                     = 0;
		$duration                                                           = 0;
		$source_info{$src}{format}{duration} =~ m/(\d+\.\d+)/ and $duration = floor( 1. + ( 1. * $1 ));

		for ( my $i = 0; $i < $source_info{$src}{format}{nb_streams}; ++$i ) {
			if ( $source_info{$src}{streams}[$i]{codec_type} eq "video" ) {
				$have_video   = 1;
				$video_stream = $i;
				$source_info{$src}{streams}[$i]{avg_frame_rate} =~ m/(\d+)\/(\d+)/ and ( 1. * $1 > 0. ) and ( 1. * $2 > 0. )
				and $avg_frame_rate = floor( 1. * ( ( 1. * $1 ) / ( 1. * $2 ) ));
			}
			if ( $source_info{$src}{streams}[$i]{codec_type} eq "audio" ) {
				if ( 0 == $have_audio ) {
					$have_audio   = 1;
					$audio_stream = $i;
					if ( $source_info{$src}{streams}[$i]{channels} > $audio_channels ) {
						$audio_channels = $source_info{$src}{streams}[$i]{channels};
						$audio_layout   = channels_to_layout( $audio_channels );
					}
				} elsif ( 0 == $have_voice ) {
					$have_voice   = 1;
					$voice_stream = $i;
					if ( $source_info{$src}{streams}[$i]{channels} > $voice_channels ) {
						$voice_channels = $source_info{$src}{streams}[$i]{channels};
						$voice_layout   = channels_to_layout( $voice_channels );
					}
				} else {
					log_error( "Found third audio channel in '%s' - no idea what to do with it!", $src );
					return 0;
				}
			}
		}
		( 0 == $have_video ) and log_error( "Source file '%s' has no video stream!", $src ) and return 0;
		if ( ( $duration > 0 ) && ( $avg_frame_rate > 0 ) &&
		     ( ( $source_info{$src}{duration} != $duration ) || ( $source_info{$src}{sourceFPS} != $avg_frame_rate ) ) ) {
			$probeDura                         = $duration * 1000 * 1000;
			$probeFPS                          = $avg_frame_rate * 8;
			$source_info{$src}{probedDuration} = $probeDura > $MaxProbeDura ? $MaxProbeDura : $probeDura;
			$source_info{$src}{probeFPS}       = $probeFPS > $MaxProbeFPS ? $MaxProbeFPS : $probeFPS;
			$source_info{$src}{probeStrings}   = sprintf( "-probesize %d -analyzeduration %d -fpsprobesize %d",
			                                              $source_info{$src}{probeSize}, $source_info{$src}{probedDuration},
			                                              $source_info{$src}{probeFPS} );
		}
	} ## End of analyzing input files

	log_debug( "Source Information gathered:\n" . Dumper( \%source_info ));

	return 1;
}

sub build_source_groups {
	my $group_id    = 0;
	my $last_dir    = ( 0 == length( $path_temp ) ) ? "n/a" : $path_temp;
	my $last_ch_cnt = 0;
	my %last_codec  = ();
	my $tmp_count   = 0;

	foreach my $fileID ( sort { $a <=> $b } keys %source_ids ) {
		can_work() or last;
		my $src  = $source_ids{$fileID};
		my $data = $source_info{$src}; ## shortcut

		# The next group is needed, if channel count, any codec or the directory changes.
		my $dir_changed   = ( 0 == length( $path_temp ) ) && ( $data->{dir} ne $last_dir );
		my $ch_changed    = ( $data->{format}{nb_streams} != $last_ch_cnt );
		my $codec_changed = 0;

		# Codecs must be looked at in a loop, as we do not know how many there are.
		for ( my $i = 0; $i < $data->{format}{nb_streams}; ++$i ) {
			( ( !defined( $last_codec{$i} ) ) or ( $last_codec{$i} ne $data->{streams}[$i]{codec_name} ) )
			and $codec_changed = 1;
			$last_codec{$i}    = $data->{streams}[$i]{codec_name};
		}
		$last_dir    = ( 0 == length( $path_temp ) ) ? $data->{dir} : $path_temp;
		$last_ch_cnt = $data->{format}{nb_streams};

		# Let's start a new group if anything changed
		if ( ( $dir_changed + $ch_changed + $codec_changed ) > 0 ) {
			$source_groups{ ++$group_id } = {
				dir  => $last_dir,
				dur  => 0,
				fps  => 0,
				idn  => sprintf( "%s/temp_%d_inter_dn_%d_%%d.mkv", $last_dir, $tmp_pid, ++$tmp_count ),
				ids  => [],
				iup  => sprintf( "%s/temp_%d_inter_up_%d_%%d.mkv", $last_dir, $tmp_pid, ++$tmp_count ),
				lst  => sprintf( "%s/temp_%d_segments_%d_src.lst", $last_dir, $tmp_pid, ++$tmp_count ),
				prg  => sprintf( "%s/temp_%d_progress_%d_%%d.prg", $last_dir, $tmp_pid, ++$tmp_count ),
				srcs => [],
				tmp  => sprintf( "%s/temp_%d_segments_%d_%%d.mkv", $last_dir, $tmp_pid, ++$tmp_count )
			};
		}

		# Now add the file
		$source_groups{$group_id}{dur} += $data->{duration};
		$data->{sourceFPS} > $source_groups{$group_id}{fps}
		and $source_groups{$group_id}{fps} = $data->{sourceFPS};
		push( @{ $source_groups{$group_id}{ids} }, $fileID );
		push( @{ $source_groups{$group_id}{srcs} }, $src );
	} ## End of grouping input files

	return 1;
}

sub can_work {
	return 0 == $death_note;
}

sub channels_to_layout {
	my ( $channels ) = @_;
	( 1 == $channels ) and return "mono";
	( 2 == $channels ) and return "stereo";
	( 3 == $channels ) and return "2.1";
	( 4 == $channels ) and return "quad";
	( 5 == $channels ) and return "4.1";
	( 6 == $channels ) and return "5.1";
	( 7 == $channels ) and return "6.1";
	( 8 == $channels ) and return "7.1";
	return "guess";
}

sub check_arguments {
	my $errcnt     = 0;
	my $total_size = 0;

	# === Pre Test: input and output must be set! ===
	# -----------------------------------------------
	my $have_source = scalar @path_source;
	my $have_target = length( $path_target );
	$have_source > 0 or log_error( "No Input given!" ) and ++$errcnt;
	$have_target > 0 or log_error( "No Output given!" ) and ++$errcnt;

	# Set the logfile according to whether we have a target or not
	if ( $have_target > 0 ) {
		$logfile = $path_target;
		$logfile =~ s/\.[^.]+$/.log/;
	}

	# === Test 1: The input file(s) must exist! ===
	# ---------------------------------------------
	if ( $have_source > 0 ) {
		foreach my $src ( @path_source ) {
			if ( -f $src ) {
				my $in_size = -s $src;
				$in_size > 0 or log_error( "Input file '%s' is empty!", $src ) and ++$errcnt;
				$total_size += $in_size / 1024 / 1024; # We count 1M blocks
				++$source_count;
			} else {
				log_error( "Input file '%s' does not exist!", $src ) and ++$errcnt;
			}
		}
	}

	# === 2: The output must not exist and must not equal any input ===
	# -----------------------------------------------------------------
	if ( $have_target > 0 ) {
		-f $path_target and log_error( "Output file '%s' already exists!", $path_target ) and ++$errcnt;
		foreach my $src ( @path_source ) {
			$src eq $path_target and log_error( "Input file '%s' equals output file!", $src ) and ++$errcnt;
		}
		$path_target =~ m/\.mkv$/ or log_error( "Output file does not have mkv ending!" ) and ++$errcnt;
	}

	# === 3: The temp directory exist and must have enough space ===
	# --------------------------------------------------------------
	if ( ( $have_target > 0 ) && ( $have_source > 0 ) ) {
		if ( length( $path_temp ) > 0 ) {
			# =) Single Temp Dir provided by User
			if ( -d $path_temp ) {
				# =) Temp Dir exists
				my $ref                = df( $path_temp );
				$dir_stats{$path_temp} = { has_space => 0, need_space => 0, srcs => [] };
				foreach my $src ( @path_source ) {
					push( @{ $dir_stats{$path_temp}{srcs} }, $src );
				}
				if ( defined( $ref ) ) {
					# The temporary UT Video files will need roughly 42-47 times the input
					# Plus a probably 3 times bigger output than input and we end at x50.
					my $needed_space = $total_size * 50;
					my $have_space   = $ref->{bavail} / 1024; # df returns 1K blocks, but we calculate in M.
					if ( $have_space < $needed_space ) {
						log_error( "Not enough space! '%s' has only %s / %s M free!",
						           $path_temp,
						           cleanint( $have_space ),
						           cleanint( $needed_space )) and ++$errcnt;
					}
				} else {
					# =) df() failed? WTH?
					log_error( "df'ing directory '%s' FAILED!", $path_temp ) and ++$errcnt;
				}
			} else {
				# =) Temp Dir does NOT exist
				log_error( "Temp directory '%s' does not exist!", $path_temp ) and ++$errcnt;
			}
		} else {
			# Default behaviour - aka no user provided temp dir
			# In this case we have to go through the input file(s) directory(ies) and calculate each individual needs.
			foreach my $src ( @path_source ) {
				if ( -f $src ) {
					my $dir                    = dirname( $src );
					length( $dir ) > 0 or $dir = ".";
					if ( !defined( $dir_stats{$dir} ) ) {
						$dir_stats{$dir} = { has_space => 0, need_space => 0, srcs => [] };
					}
					push( @{ $dir_stats{$dir}{srcs} }, $src );
					my $ref = df( $dir );
					if ( defined( $ref ) ) {
						$dir_stats{$dir}{has_space}  += $ref->{bavail} / 1024;     ## again count in M not K
						$dir_stats{$dir}{need_space} += ( -s $src ) / 1024 / 1024; ## also in M now.
					} else {
						# =) df() failed? WTF?
						log_error( "df'ing directory '%s' FAILED!", $dir ) and ++$errcnt;
					}
				} # No else, that error has already been recorded under Test 1
			}
			## Now check the stats...
			foreach my $dir ( sort keys %dir_stats ) {
				$dir_stats{$dir}{need_space} > $dir_stats{$dir}{has_space}
				and log_error( "Not enough space! '%s' has only %s / %s M free!",
				               $dir,
				               cleanint( $dir_stats{$dir}{has_space} ),
				               cleanint( $dir_stats{$dir}{need_space} ))
				and ++$errcnt;
			}
		}
	}

	return $errcnt;
}

sub cleanint {
	my ( $float ) = @_;
	my $int       = floor( $float );
	return commify( $int );
}

sub create_target_file {
	my ( $lstfile, $prgfile, $mapfile ) = @_;
	can_work() or return 1;

	# The filters are only for keeping full color ranges
	my $F_in_scale  = "scale='in_range=full:out_range=full'";
	my $F_out_scale = "scale='flags=accurate_rnd+full_chroma_inp+full_chroma_int:in_range=full:out_range=full'";
	my $F_assembled = "${B_in}${F_in_scale}${B_middle}${F_out_scale}${B_out}";

	# If we have a second stream, it is the voice-under that has to be stored in a separate file
	my @mapVoice = ();
	if ( ( $do_split_audio > 0 ) && ( $voice_stream > -1 ) ) {
		@mapVoice = ( "-map", "0:$voice_stream", qw( -vn -codec:a:0 pcm_s24le ) );
		( 2 != $voice_channels ) and push( @mapVoice, qw( -channel_layout:a:0 stereo -ac:a:0 2 ) );
		push( @mapVoice, $mapfile );
	}

	# The main audio is probably in 7.1 and we need 5.1 in channel 0 and stereo in channel 1
	my @mapAudio  = ( qw( -map 0:0 -map ), "0:$audio_stream", qw( -codec:a:0 pcm_s24le ) );
	my @metaAudio = qw( -map_metadata 0 -metadata:s:a:0 title=Stereo -metadata:s:a:0 language=eng );

	if ( 2 < $audio_channels ) {
		push( @mapAudio, ( qw( -channel_layout:a:0 5.1 -ac:a:0 6 -map ), "0:$audio_stream", qw( -codec:a:1 pcm_s24le -channel_layout:a:1 stereo -ac:a:1 2 ) ));
		@metaAudio = qw( -map_metadata 0 -metadata:s:a:0 title=Surround -metadata:s:a:0 language=eng -metadata:s:a:1 title=Stereo -metadata:s:a:1 language=eng );
	}

	# Building the four worker threads is quite trivial
	can_work() or return 1;
	my @ffargs = ( $FF, @FF_ARGS_START, "-progress", $prgfile,
	               ( ( "guess" ne $audio_layout ) ? ( "-guess_layout_max", "0" ) : () ),
	               @FF_ARGS_INPUT_CUDA, $lstfile,
	               @mapAudio, @metaAudio, @FF_ARGS_FILTER, $F_assembled, "-fps_mode", "vfr", @FF_ARGS_FORMAT,
	               @FF_ARGS_CODEC_h264, $path_target, @mapVoice );

	log_debug( "Starting Thread for:\n%s", join( ' ', @ffargs ));
	#@type Thread
	my $worker = threads->create( \&worker, 0, @ffargs );

	# Watch and join
	return watch_my_threads( {
		arg => [ \@ffargs ],
		cnt => 1,
		prg => [ $prgfile ],
		src => "",
		tgt => "",
		thr => [ $worker ]
	} );
}

sub commify {
	my ( $text ) = @_;
	$text        = reverse $text;
	$text =~ s/(\d\d\d)(?=\d)(?!\d*\.)/$1,/g;
	return scalar reverse $text;
}

sub declare_single_source {
	my $src      = $path_source[0];
	my $data     = $source_info{$src}; ## shortcut
	my $fileID   = $data->{id};
	my $last_dir = ( 0 == length( $path_temp ) ) ? $data->{dir} : $path_temp;

	$source_groups{ 0 } = {
		dir  => $last_dir,
		dur  => $data->{duration},
		fps  => $data->{sourceFPS},
		idn  => sprintf( "%s/temp_%d_inter_dn_%d_%%d.mkv", $last_dir, $tmp_pid, 1 ),
		ids  => [ $fileID ],
		iup  => sprintf( "%s/temp_%d_inter_up_%d_%%d.mkv", $last_dir, $tmp_pid, 2 ),
		lst  => sprintf( "%s/temp_%d_segments_%d_src.lst", $last_dir, $tmp_pid, 3 ),
		prg  => sprintf( "%s/temp_%d_progress_%d_%%d.prg", $last_dir, $tmp_pid, 4 ),
		srcs => [ $src ],
		tmp  => sprintf( "%s/temp_%d_segments_%d_%%d.mkv", $last_dir, $tmp_pid, 5 )
	};

	return 1;
}


# A die handler that lets perl death notes be printed via log
sub dieHandler {
	my ( $err ) = @_;
	$ret_global = 42;
	return log_error( "%s", $err );
}

sub floor {
	my ( $float ) = @_;
	return sprintf( "%d", $float );
}

sub format_bitrate {
	my ( $float ) = @_;
	return lc( human_readable_size( $float )) . "bits/s";
}

sub format_out_time {
	my ( $ms ) = @_;
	my $sec    = floor( $ms / 1000000 );
	my $min    = floor( $sec / 60 );
	my $hr     = floor( $min / 60 );

	return sprintf( "%02d:%02d:%02d.%06d", $hr, $min % 60, $sec % 60, $ms % 1000000 );
}

sub human_readable_size {
	my ( $number_string ) = @_;
	my $int               = floor( $number_string );
	my @exps              = qw( B K M G T P E Z );
	my $exp               = 0;

	while ( $int >= 1024 ) {
		++$exp;
		$int /= 1024;
	}

	return sprintf( "%3.2f%s", floor( $int * 100. ) / 100., $exps[$exp] );
}

sub interpolate_source_group {
	my ( $gid, $tmp_from, $tmp_to, $dec_max, $dec_frac, $tgt_fps, $filter_extra ) = @_;
	defined( $filter_extra ) or $filter_extra                                     = "";
	defined( $source_groups{$gid} ) or log_error( "Source Group ID %d does not exist!", $gid ) and return 0;
	can_work() or return 1;

	# We do not need any fancy loops here, because there will always be 4 segments, and thus 4 threads. No exceptions.
	#@type Thread
	my @workers = ( undef, undef, undef, undef );
	my @prgLogs = (
		sprintf( $source_groups{$gid}{prg}, 0 ),
		sprintf( $source_groups{$gid}{prg}, 1 ),
		sprintf( $source_groups{$gid}{prg}, 2 ),
		sprintf( $source_groups{$gid}{prg}, 3 )
	);

	my $F_in_scale    = "${filter_extra}scale='in_range=full:out_range=full'";
	my $F_scale_FPS   = "fps=${tgt_fps}:round=near";
	my $F_mpdecimate  = "mpdecimate='max=${dec_max}:frac=${dec_frac}'";
	my $F_out_scale   = "scale='flags=accurate_rnd+full_chroma_inp+full_chroma_int:in_range=full:out_range=full'";
	my $F_interpolate = "libplacebo='extra_opts=preset=high_quality:frame_mixer=" .
	                    ( ( "iup" eq $tmp_to ) ? "mitchell_clamp" : "oversample" ) .
	                    ":fps=${tgt_fps}'";
	#my $F_assembled   = "${B_in}${F_in_scale}${B_FPS}${F_scale_FPS}${B_decimate}${F_mpdecimate}${B_middle}${F_out_scale}${B_interp}${F_interpolate}${B_out}";
	my $F_assembled = "${B_in}${F_in_scale}" .
	                  ( ( "iup" eq $tmp_to ) ? "${B_FPS}${F_scale_FPS}" : "" ) .
	                  "${B_decimate}${F_mpdecimate}${B_middle}${F_out_scale}${B_interp}${F_interpolate}${B_out}";

	# Building the four worker threads is quite trivial
	can_work() or return 1;
	my @ffargs  = ( [], [], [], [] );
	for ( my $i = 0; $i < 4; ++$i ) {
		# Let's build the command line arguments:
		$ffargs[$i] = [ $FF, @FF_ARGS_START, "-progress", $prgLogs[$i],
		                ( ( "guess" ne $audio_layout ) ? ( "-guess_layout_max", "0" ) : () ),
		                @FF_ARGS_INPUT_VULK, sprintf( $source_groups{$gid}{$tmp_from}, $i ),
		                @FF_ARGS_ACOPY_FIL, $F_assembled, "-fps_mode", "cfr", @FF_ARGS_FORMAT, @FF_ARGS_CODEC_UTV,
		                sprintf( $source_groups{$gid}{$tmp_to}, $i )
		];

		log_debug( "Starting Thread for:\n%s", join( ' ', @ffargs ));
		#@type Thread
		$workers[$i] = threads->create( \&worker, $i, @{ $ffargs[$i] } );
	}

	# Watch and join
	return watch_my_threads( {
		arg => \@ffargs,
		cnt => 4,
		prg => \@prgLogs,
		src => $source_groups{$gid}{$tmp_from},
		tgt => $source_groups{$gid}{$tmp_to},
		thr => \@workers
	} );
}

# Load data from between the last two "progress=<state>" lines in the given log file, and store it in the given hash
# If the hash has values, progress data is added.
sub load_progress {
	my ( $prgLog, $prgData ) = @_;

	# Check/Initialize the progress hash
	defined( $prgData->{bitrate} ) or $prgData->{bitrate}         = 0.0; ## "0.0kbits/s" in the file
	defined( $prgData->{drop_frames} ) or $prgData->{drop_frames} = 0;
	defined( $prgData->{dup_frames} ) or $prgData->{dup_frames}   = 0;
	defined( $prgData->{fps} ) or $prgData->{fps}                 = 0.0;
	defined( $prgData->{frames} ) or $prgData->{frames}           = 0;
	defined( $prgData->{out_time} ) or $prgData->{out_time}       = 0; ## "00:00:00.000000" in the file, but we read out_time_ms
	defined( $prgData->{total_size} ) or $prgData->{total_size}   = 0;

	# Leave early if the log file is not there (yet)
	( ( 0 == length( $prgLog ) ) || ( !-f $prgLog ) || ( 0 == -s $prgLog ) || !open( my $fIn, "<", $prgLog ) ) and return 0;
	close( $fIn ); # We do not read it like that, it was just for testing if the file cvan be opened.

	my $line_num    = 0;
	my $progress_no = 0;

	# Suck up the last 20 lines (This *should* be enough to get 2 progress=xxx lines)
	my @args     = ( "tail", "-n", "20", $prgLog );
	my @lines    = reverse capturex( @args );
	my $line_cnt = scalar @lines;

	# Go beyond first progress=xxx line found
	while ( ( $progress_no < 1 ) && ( $line_num < $line_cnt ) ) {
		chomp $lines[$line_num];
		#log_debug( "Progress Line %d: '%s'", $line_num + 1, $lines[$line_num] );
		$lines[$line_num] =~ m/^progress=/ and ++$progress_no;
		++$line_num;
	}

	# Now load everything until a second progress=xxx line is found
	while ( ( $progress_no < 2 ) && ( $line_num < $line_cnt ) ) {
		chomp $lines[$line_num];
		#log_debug( "Progress Line %d: '%s'", $line_num + 1, $lines[$line_num] );

		$lines[$line_num] =~ m/^bitrate=(\d+\.?\d*)\D\S+\s*$/ and $prgData->{bitrate} += ( 1. * $1 ) and ++$line_num and next;
		$lines[$line_num] =~ m/^drop_frames=(\S+)\s*$/ and $prgData->{drop_frames}    += ( 1 * $1 ) and ++$line_num and next;
		$lines[$line_num] =~ m/^dup_frames=(\S+)\s*$/ and $prgData->{dup_frames}      += ( 1 * $1 ) and ++$line_num and next;
		$lines[$line_num] =~ m/^fps=(\S+)\s*$/ and $prgData->{fps}                    += ( 1. * $1 ) and ++$line_num and next;
		$lines[$line_num] =~ m/^frame=(\S+)\s*$/ and $prgData->{frames}               += ( 1 * $1 ) and ++$line_num and next;
		$lines[$line_num] =~ m/^out_time_ms=(\d+)\s*$/ and $prgData->{out_time}       += ( 1 * $1 ) and ++$line_num and next;
		$lines[$line_num] =~ m/^total_size=(\d+)\s*$/ and $prgData->{total_size}      += ( 1 * $1 ) and ++$line_num and next;

		$lines[$line_num] =~ m/^progress=/ and ++$progress_no;

		++$line_num;
	}

	return ( 2 == $progress_no ) ? 1 : 0; ## If the progress line count is 0 or 1, the process is not (really) started (, yet)
}

sub logMsg {
	my ( $lvl, $fmt, @args ) = @_;

	defined( $lvl ) or $lvl = 2;

	( $LOG_DEBUG == $lvl ) and ( 0 == $do_debug ) and return 1;

	my @tLocalTime = localtime();
	my $stTime     =
		sprintf( "%04d-%02d-%02d %02d:%02d:%02d", $tLocalTime[5] + 1900, $tLocalTime[4] + 1, $tLocalTime[3], $tLocalTime[2], $tLocalTime[1], $tLocalTime[0] );

	my $stLevel = "=DEBUG=: ";
	$stLevel    = "Info   : " if ( $lvl == $LOG_INFO );
	$stLevel    = "Warning: " if ( $lvl == $LOG_WARNING );
	$stLevel    = "ERROR  : " if ( $lvl == $LOG_ERROR );
	$stLevel    = "" if ( $lvl == $LOG_STATUS );

	my $stMsg = sprintf( "%s|%s$fmt", $stTime, $stLevel, @args );
	if ( ( 0 < length( $logfile ) ) && open( my $fLog, ">>", $logfile ) ) {
		print $fLog ( "${stMsg}\n" ) and close( $fLog );
	}

	if ( $have_progress_msg > 0 ) {
		print "\n";
		$have_progress_msg = 0;
	}

	local $| = 1;
	print "${stMsg}\n";

	return 1;
} ## end sub logMsg

sub log_info {
	my ( $fmt, @args ) = @_;
	return logMsg( $LOG_INFO, $fmt, @args );
}

sub log_warning {
	my ( $fmt, @args ) = @_;
	return logMsg( $LOG_WARNING, $fmt, @args );
}

sub log_error {
	my ( $fmt, @args ) = @_;
	$ret_global        = 1;
	return logMsg( $LOG_ERROR, $fmt, @args );
}

sub log_status {
	my ( $fmt, @args ) = @_;
	return logMsg( $LOG_STATUS, $fmt, @args );
}

sub log_debug {
	my ( $fmt, @args ) = @_;
	$do_debug or return 1;
	return logMsg( $LOG_DEBUG, $fmt, @args );
} ## end sub log_debug

# A signal handler that sets global vars according to the signal given.
# Unknown signals are ignored.
sub sigHandler {
	my ( $sig ) = @_;
	if ( "INT" eq $sig ) {
		## CTRL+C
		$death_note = 1;
		log_warning( "Caught Interrupt Signal - Ending Tasks..." );
	} elsif ( "QUIT" eq $sig ) {
		$death_note = 1;
		log_warning( "Caught Quit Signal - Ending Tasks..." );
	} elsif ( "TERM" eq $sig ) {
		$death_note = 1;
		log_warning( "Caught Terminate Signal - Ending Tasks..." );
	} else {
		log_warning( "Caught Unknown Signal [%s] ... ignoring Signal!", $sig );
	}

	return 1;
} ## end sub sigHandler


sub segment_source_group {
	my ( $gid, $prgfile ) = @_;
	defined( $source_groups{$gid} ) or log_error( "Source Group ID %d does not exist!", $gid ) and return 0;
	can_work() or return 1;

	# We use this to check on the overall maximum fps
	( $source_groups{$gid}{fps} > $max_fps ) and $max_fps = $source_groups{$gid}{fps};

	# Each segment must be a quarter of the total duration, raised to the next full second
	my $seg_len = floor( 1. + ( $source_groups{$gid}{dur} / 4. ));

	# Luckily we can concat and segment in one go, but we need the concat demuxer for that, which requires an input file
	if ( open( my $fOut, ">", $source_groups{$gid}{lst} ) ) {
		foreach my $fid ( sort { $a <=> $b } @{ $source_groups{$gid}{ids} } ) {
			printf( $fOut "file '%s'\n", $source_ids{$fid} );
		}
		close( $fOut );
	} else {
		log_error( "Cannot write list file '%s': %s", $source_groups{$gid}{lst}, $! );
		return 0;
	}

	# Let's build the command line arguments:
	my @ffargs = ( $FF, @FF_ARGS_START, "-progress", $prgfile,
	               ( ( "guess" ne $audio_layout ) ? ( "-guess_layout_max", "0" ) : () ),
	               @FF_CONCAT_BEGIN, $source_groups{$gid}{lst}, @FF_CONCAT_END,
	               "-f", "segment", "-segment_time", "$seg_len",
	               $source_groups{$gid}{tmp} );

	log_debug( "Starting Thread for:\n%s", join( ' ', @ffargs ));
	#@type Thread
	my $worker = threads->create( \&worker, 0, @ffargs );

	# Watch and join
	my $result = watch_my_threads( {
		arg => [ \@ffargs ],
		cnt => 1,
		prg => [ $prgfile ],
		src => "",
		tgt => "",
		thr => [ $worker ]
	} );

	# The list file is no longer needed.
	-f $source_groups{$gid}{lst} and ( 0 == $do_debug ) and unlink( $source_groups{$gid}{lst} );

	return $result;
}


# Show data from between the last two "progress=<state>" lines in the given log file
sub show_progress {
	my ( $thr_count, $thr_active, $prgData, $log_as_info ) = @_;

	# Formualate the progress line
	my $size_str    = human_readable_size( $prgData->{total_size} // 0 );
	my $time_str    = format_out_time( $prgData->{out_time} // 0 );
	my $bitrate_str = format_bitrate( ( $prgData->{bitrate} // 0.0 ) / $thr_count ); ## Average, not the sum.

	# This is a bit paranoid, but a good bug-finder
	my $killme                                                                                                  = 0;
	defined( $thr_active ) or log_error( "%s NOT DEFINED", "\$thr_active" ) and $killme                         = 1;
	defined( $prgData->{frames} ) or log_error( "%s NOT DEFINED", "\$prgData->{frames}" ) and $killme           = 1;
	defined( $prgData->{drop_frames} ) or log_error( "%s NOT DEFINED", "\$prgData->{drop_frames}" ) and $killme = 1;
	defined( $prgData->{dup_frames} ) or log_error( "%s NOT DEFINED", "\$prgData->{dup_frames}" ) and $killme   = 1;
	defined( $time_str ) or log_error( "%s NOT DEFINED", "\$time_str" ) and $killme                             = 1;
	defined( $prgData->{fps} ) or log_error( "%s NOT DEFINED", "\$prgData->{fps}" ) and $killme                 = 1;
	defined( $bitrate_str ) or log_error( "%s NOT DEFINED", "\$bitrate_str" ) and $killme                       = 1;
	defined( $size_str ) or log_error( "%s NOT DEFINED", "\$size_str" ) and $killme                             = 1;
	( 0 == $killme ) or exit 99;

	my $progress_str = sprintf( "[%d running] Frame %d (%d drp, %d dup); %s; FPS: %03.2f; %s; File Size: %s    ",
	                            $thr_active, $prgData->{frames}, $prgData->{drop_frames}, $prgData->{dup_frames}, $time_str, $prgData->{fps}, $bitrate_str, $size_str );

	# Clear a previous progress line
	( $have_progress_msg > 0 ) and print "\r" . ( ' ' x length( $progress_str ) ) . "\r";

	if ( 0 < $log_as_info ) {
		# Write into log file
		$have_progress_msg = 0;
		log_info( "%s", $progress_str );
	} else {
		# Output on console
		$have_progress_msg = 1;
		local $|           = 1;
		print "${progress_str}";
	}

	return 1;
}


# A warnings handler that lets perl warnings be printed via log
sub warnHandler {
	my ( $warn ) = @_;
	return log_warning( "%s", $warn );
}


# This is a watchdog function that displays progress and joins all threads nicely if needed
sub watch_my_threads {
	my ( $data )     = @_;
	my $result       = 1;
	my @ffargs       = @{ $data->{arg} // [ [], [], [], [] ] };
	my @prgLogs      = @{ $data->{prg} // [] };
	my $src_fmt      = $data->{src} // "unknown_src_%d.mkv";
	my $tgt_fmt      = $data->{tgt} // "unknown_tgt_%d.mkv";
	my @workers      = @{ $data->{thr} // [] };
	my $thr_count    = $data->{cnt} // scalar @workers;
	my $thr_active   = $thr_count;
	my @thr_progress = ( 20, 20, 20, 20 );

	# The whole watching is done in two phases.
	log_debug( "Progress Logs: %s", join( ', ', @prgLogs ));
	log_debug( "Source FMT   : %s", $src_fmt );
	log_debug( "Target FMT   : %s", $tgt_fmt );
	log_debug( "Threads      : %s", join( ', ', @workers ));

	# Phase 1: show logged progress as long as there are non-joinable threads left
	while ( $thr_active > 0 ) {
		my %prgData = ();

		# Load the current progress data
		for ( my $i = 0; $i < $thr_count; ++$i ) {
			if ( $workers[$i]->is_joinable() && ( $THR_FINISHED == $work_status[$i] ) ) {
				--$thr_active;
				$work_status[$i] = $THR_STOPPED; # Thread acknowledged as being stopped
			}

			# Load progress of the current thread
			load_progress( $prgLogs[$i], \%prgData ) or --$thr_progress[$i];
			yield();

			# If the thread has not been started, start it now
			( $THR_CREATED == $work_status[$i] ) and $work_status[$i] = $THR_RUNNING
			and $thr_progress[$i]                                     = 20; ## reset the progress counter
		}                                                                   ## end of looping threads

		# If all threads are inactive now, log the final progress
		( 0 == $thr_active ) and show_progress( $thr_count, $thr_active, \%prgData, 1 ) and next;

		# Otherwise show the accumulated progress data
		show_progress( $thr_count, $thr_active, \%prgData, 0 );

		# if any thread has not returned 0 from load_progress() for 10 seconds, we consider it frozen
		for ( my $i = 0; $i < $thr_count; ++$i ) {
			( ( $thr_progress[$i] > 0 ) or ( $work_status[$i] != $THR_RUNNING ) ) and next;

			# We do this in five phases, but check them in reverse:
			if ( -4 > $thr_progress[$i] ) {
				# Phase 5: It is time to restart the thread
				log_warning( "Re-starting frozen thread %d", $i );
				$workers[$i] = threads->create( \&worker, $i, @{ $ffargs[$i] } );
			} elsif ( -3 == $thr_progress[$i] ) {
				# Phase 4: We had two termination and a kill signal. It is time to detach the thread
				if ( defined( $workers[$i] ) ) {
					if ( $workers[$i]->is_joinable() ) {
						log_warning( "Joining frozen thread %d", $i );
						$workers[$i]->join();
					} elsif ( !$workers[$i]->is_detached() ) {
						log_warning( "DETACHING frozen thread %d", $i );
						$workers[$i]->detach();
					}
					undef( $workers[$i] );
				}
				$work_status[$i] = $THR_INACTIVE;
				-f $prgLogs[$i] and unlink( $prgLogs[$i] );
			} elsif ( -2 == $thr_progress[$i] ) {
				# Phase 3: Send a SIGKILL
				defined( $workers[$i] ) and $workers[$i]->is_running() and log_warning( "Sending SIGKILL to frozen thread %d", $i )
				and $workers[$i]->kill( 'SIGKILL' ) or $thr_progress[$i] = -3;
			} elsif ( -1 == $thr_progress[$i] ) {
				# Phase 2: Send another SIGTERM
				defined( $workers[$i] ) and $workers[$i]->is_running() and log_warning( "Sending second SIGTERM to frozen thread %d", $i )
				and $workers[$i]->kill( 'SIGTERM' ) or $thr_progress[$i] = -3;
			} elsif ( 0 == $thr_progress[$i] ) {
				# Phase 1: Send a SIGTERM
				defined( $workers[$i] ) and $workers[$i]->is_running() and log_warning( "Sending first SIGTERM to frozen thread %d", $i )
				and $workers[$i]->kill( 'SIGTERM' ) or $thr_progress[$i] = -3;
			}
		}

		# Sleep for half a second before going back to the loop start
		usleep( 500000 );
	} ## End of showing threads progress

	# Reset active count, as we have to count them down again
	$thr_active = $thr_count;

	# Phase 2: Join all Threads back and handle result values
	while ( $thr_active > 0 ) {
		for ( my $i = 0; $i < $thr_count; ++$i ) {
			if ( $workers[$i]->is_joinable() && ( $THR_STOPPED == $work_status[$i] ) ) {
				my $res          = $workers[$i]->join();
				$work_status[$i] = $THR_JOINED;
				if ( $res != 0 ) {
					log_error( "Thread %d failed [%d]", $i, $res );
					$result = 0;

					# We do not need the target file any more, the thread failed! (if an fmt is set)
					if ( 0 == $do_debug ) {
						if ( 0 < length( $tgt_fmt ) ) {
							my $f = sprintf( $tgt_fmt, $i );
							log_debug( "Removing temp target file '%s' ...", $f );
							-f $f and unlink( $f );
						}
					}
				}
				--$thr_active;

				# We do not need the source file any more (if an fmt is set)
				if ( 0 == $do_debug ) {
					if ( 0 < length( $src_fmt ) ) {
						my $f = sprintf( $src_fmt, $i );
						log_debug( "Removing temp source file '%s' ...", $f );
						-f $f and unlink( $f );
					}
				}
			}
			yield();
		} # end of looping threads
	}     ## End of joining worker threads

	# Eventually mark all threads as inactive / gone
	for ( my $i = 0; $i < $thr_count; ++$i ) {
		defined( $data->{thr}[$i] ) and undef( $data->{thr}[$i] );
		$work_status[$i] = $THR_INACTIVE;
	}

	return $result;
}


__END__

=head1 cac

[c]leanup [a]nd [c]onvert: Overhaul Clips shot by HurryKane for SedGaming channel

The program uses ffmpeg to remove duplicate frames and to interpolate the video to twice the target
FPS in a first step, then do another search for duplicate frames and interpolate down the the
target FPS.

If the source has at least 50 FPS in average, the target is set to 60 FPS. For sources with less
than 50 FPS in average, the target is set to 30 FPS.
You can use the -u/--upgrade option to force the target to be 60 FPS, no matter the source average.

=head1 SYNOPSIS

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
=cut

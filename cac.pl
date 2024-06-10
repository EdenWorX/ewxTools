#!/usr/bin/perl -w
use strict;
use warnings FATAL => 'all';

use PerlIO;
use POSIX qw( _exit floor :sys_wait_h );
use IPC::Run3;
use IPC::Shareable qw( :lock );
use Carp;
use Data::Dumper;
use File::Basename;
use Filesys::Df;
use Getopt::Long;
use List::MoreUtils qw( firstidx );
use Pod::Usage;
use Readonly;
use Time::HiRes qw( usleep );

my $work_done = 0;

# ===============
# === HISTORY ===
# ===============
# Version  Date        Maintainer     Changes
# 1.0.0    2024-05-23  sed, EdenWorX  First fully working version of the per variant. The Bash variant is dead now.
# 1.0.1    2024-05-29  sed, EdenWorX  Use IPC::Cmd::run_forked instead of using system()
# 1.0.2    2024-05-30  sed, EdenWorX  Rewrote the workers to be true forks instead of using iThreads.
#
# Please keep this current:
our $VERSION = '1.0.2';


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
# signal handling
my $death_note = 0;

# Global return value, is set to 1 by log_error()
my $ret_global = 0;

Readonly my $FF_CREATED => 1;
Readonly my $FF_RUNNING => 2;
Readonly my $FF_FINISHED => 3;
Readonly my $FF_REAPED => 5;

#@type IPC::Shareable
my $work_data = IPC::Shareable->new( key => 'WORK_DATA', create => 1 );
#@type IPC::Shareable
my $work_lock      = tied %{ $work_data };
$work_data->{cnt}  = 0;
$work_data->{PIDs} = {};

Readonly my $EMPTY => q{};
Readonly my $SPACE => q{ };


# ---------------------------------------------------------
# Logging facilities
# ---------------------------------------------------------
my $do_debug          = 0;
my $have_progress_msg = 0;
my $logfile           = $EMPTY;

Readonly my $LOG_DEBUG => 1;
Readonly my $LOG_STATUS => 2;
Readonly my $LOG_INFO => 3;
Readonly my $LOG_WARNING => 4;
Readonly my $LOG_ERROR => 5;


# ---------------------------------------------------------
# Signal Handlers
# ---------------------------------------------------------
my %signalHandlers = (
	'INT'  => sub {
		$death_note = 1;
		log_warning( 'Caught Interrupt Signal - Ending Tasks...' );
	},
	'QUIT' => sub {
		$death_note = 1;
		log_warning( 'Caught Quit Signal - Ending Tasks...' );
	},
	'TERM' => sub {
		$death_note = 1;
		log_warning( 'Caught Terminate Signal - Ending Tasks...' );
	}
);
local $SIG{INT}  = \&sigHandler;
local $SIG{QUIT} = \&sigHandler;
local $SIG{TERM} = \&sigHandler;

# Warnings should be logged, too:
$SIG{__WARN__} = \&warnHandler;

# And fatal errors go to the log as well
$SIG{__DIE__} = \&dieHandler;

# Global SIGCHLD handler
$SIG{CHLD} = \&reaper;


# ---------------------------------------------------------
# Global Constants
# ---------------------------------------------------------
Readonly my $B_FPS => '[fps];[fps]';
Readonly my $B_decimate => '[decim];[decim]';
Readonly my $B_in => '[in]';
Readonly my $B_interp => '[interp];[interp]';
Readonly my $B_middle => '[middle];[middle]';
Readonly my $B_out => '[out]';
Readonly my $defaultProbeSize => 256 * 1_024 * 1_024; # Max out probe size at 256 MB, all relevant stream info should be available from that size
Readonly my $defaultProbeDura => 30 * 1_000 * 1_000;  # Max out analyze duration at 30 seconds. This should be long enough for everything
Readonly my $defaultProbeFPS => 8 * 120;              # FPS probing is maxed at 8 seconds for 120 FPS recordings.
Readonly my $TIMEOUT_INTERVALS => 60;                 # Timeout for forks to start working (60 interval = 30 seconds)


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
my $maxProbeSize       = $defaultProbeSize;
my $maxProbeDura       = $defaultProbeDura;
my $maxProbeFPS        = $defaultProbeFPS;
my $target_fps         = 0;
my @path_source        = ();
my $path_target        = $EMPTY;
my $path_temp          = $EMPTY;
my $source_count       = 0;
my %source_groups      = ();
my %source_ids         = ();
my %source_info        = ();
my $tmp_pid            = $$;
my $video_stream       = 0;
my $audio_stream       = 0;
my $voice_stream       = -1;
my $audio_channels     = 0;
my $audio_layout       = 'guess';
my $voice_channels     = 0;
my $voice_layout       = 'guess';


# ---------------------------------------------------------
# BEGIN Handler
# ---------------------------------------------------------
BEGIN {
	# ffmpeg default values
	chomp( $FF = qx{which ffmpeg} );
	chomp( $FP = qx{which ffprobe} );
} ## End BEGIN


# ---------------------------------------------------------
# Argument handling
# ---------------------------------------------------------
my $podmsg          = "\tcac ; HurryKane's [c]leanup [a]nd [c]onvert\n";
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
GetOptions( %program_options ) or pod2usage( { -message => $podmsg, -exitval => 2, -verbose => 0 } );
$do_print_help > 0 and pod2usage( { -exitval => 0, -verbose => 2, -noperldoc => 1 } );
$do_print_version > 0 and print "EWX cac V$VERSION\n" and exit 0;


# ---------------------------------------------------------
# Check Arguments
# ---------------------------------------------------------
check_arguments() > 0 and pod2usage( { -message => $podmsg, -exitval => 1, -verbose => 0 } ); ## The sub has already logged

defined $FF and ( 0 < ( length $FF ) ) and -x $FF or log_error( q{No ffmpeg available (FF: '%s')}, $FF // 'undef' ) and exit 3;
defined $FP and ( 0 < ( length $FP ) ) and -x $FP or log_error( q{No ffprobe available (FP: '%s')}, $FP // 'undef' ) and exit 3;


# ---------------------------------------------------------
# ================	  MAIN  PROGRAM	  ================
# ---------------------------------------------------------
$work_done = 1; # From now on we consider this program as at work
log_info( 'Processing %s start', $path_target );


# ---
# --- 1) we need information about each source file
# ---
analyze_all_inputs();


# ---
# --- 2) All input files per temp directory have to be grouped. Each group is then segmented
# ---    Into four parts, so that four threads can do the interpolation in parallel per group.
# ---
build_source_groups() or declare_single_source() or exit 7;
segment_all_groups();
check_target_fps();


# ---
# --- 3) Now each groups segments can be decimated and interpolated up to max fps (round 1)
# ---
can_work() and log_info( 'Interpolating segments up to %d FPS...', $max_fps );
foreach my $groupID ( sort { $a <=> $b } keys %source_groups ) {
	can_work() or last;
	my $filter_string = make_filter_string( 'iup', 7, 0.5, $max_fps );
	interpolate_source_group( $groupID, 'tmp', 'iup', "pad=ceil(iw/2)*2:ceil(ih/2)*2,${filter_string}" ) or exit 9;
}


# ---
# --- 4) Then all groups segments have to be decimated and interpolated down to target fps (round 2)
# ---
can_work() and log_info( 'Interpolating segments down to %d FPS...', $target_fps );
foreach my $groupID ( sort { $a <=> $b } keys %source_groups ) {
	can_work() or last;
	my $filter_string = make_filter_string( 'iup', 3, 0.667, $target_fps );
	interpolate_source_group( $groupID, 'iup', 'idn', "pad=ceil(iw/2)*2:ceil(ih/2)*2,${filter_string}" ) or exit 10;
}


# ---
# --- 5) And finally we can put all the latest temp files together and create the target vid
# ---
can_work() and log_info( 'Creating %s ...', $path_target );
assemble_output();


# ---------------------------------------------------------
# END Handler
# ---------------------------------------------------------
END {
	# Let's clean up and remove all temporary files, if this is "release" mode,
	# or at least list all "orphaned" files if this is debug mode
	if ( $work_done > 0 ) {
		foreach my $gid ( sort { $a <=> $b } keys %source_groups ) {
			if ( -f $source_groups{$gid}{lst} ) {
				( $do_debug > 0 ) and log_debug( 'See: %s', $source_groups{$gid}{lst} ) or unlink $source_groups{$gid}{lst};
			}
			for my $area ( qw( tmp idn iup prg ) ) {
				for my $i ( 0 .. 3 ) {
					my $f = sprintf $source_groups{$gid}{$area}, $i;
					if ( -f $f ) {
						( $do_debug > 0 ) and log_debug( 'See: %s', $f ) or unlink $f;
					}
				}
			}
		}

		( $ret_global > 0 ) and log_error( 'Processing %s FAILED!', $path_target ) or log_info( 'Processing %s finished', $path_target );

		( ( 0 < ( length $logfile ) ) && ( -f $logfile ) )
		and ( ( $ret_global > 0 ) || ( 1 == $do_debug ) )
		and printf "\nSee %s for details\n", $logfile;
	}
	IPC::Shareable->clean_up;
} ## End END


exit $ret_global;


# ---------------------------------------------------------
# ================ FUNCTION IMPLEMENTATION ================
# ---------------------------------------------------------

sub add_pid {
	my ( $pid ) = @_;
	defined $pid and ( $pid =~ m/^\d+$/ms )
	or log_error( "add_pid(): BUG! '%s' is not a valid pid!", $pid // 'undef' ) and confess( 'FATAL BUG!' );
	defined( $work_data->{PIDs}{$pid} ) and confess( "add_pid($pid) called but work_data already defined!" );
	$work_lock->lock;
	$work_data->{PIDs}{$pid} = {
		args      => [], ## Shall be added by the caller as a reference
		exit_code => 0,
		id        => 0,
		prgfile   => $EMPTY,
		result    => $EMPTY,
		status    => 0,
		source    => $EMPTY,
		target    => $EMPTY
	};
	$work_data->{cnt}++;
	$work_lock->unlock;
	return 1;
}

sub analyze_all_inputs {
	can_work() or return 1;
	my $pathID = 0; ## Counter for the source id hash

	foreach my $src ( @path_source ) {
		can_work() or last;

		my $inSize = -s $src;

		## no critic (ProhibitParensWithBuiltins)
		$source_info{$src} = {
			avg_frame_rate => 0,
			dir            => dirname( $src ),
			duration       => 0,
			id             => ++$pathID,
			probeSize      => $inSize > $maxProbeSize ? $maxProbeSize : $inSize,
			probeStrings   => sprintf( '-probesize %d', $inSize > $maxProbeSize ? $maxProbeSize : $inSize )
		};
		$source_ids{$pathID} = $src;

		analyze_input( $src ) or exit 6;
	}

	return 1;
}

sub analyze_input {
	my ( $src ) = @_;

	my $formats = $source_info{$src};  ## Shortcut
	my $streams = $formats->{streams}; ## shortcut, too

	# Get basic duration
	my $stream_fields = 'avg_frame_rate,duration';
	my $frstream_no   = get_info_from_ffprobe( $src, $stream_fields );
	( $frstream_no >= 0 ) or return 0;       ## Something went wrong
	my $frstream = $streams->[$frstream_no]; ## shortcut, three

	( $formats->{duration} > 0 ) or log_error( "Unable to determine duration of '%s'", $src ) and return 0;
	( $frstream->{avg_frame_rate} > 0 ) or log_error( "Unable to determine average frame rate of '%s'", $src ) and return 0;

	log_debug( 'Duration   : %d', $formats->{duration} );
	log_debug( 'Average FPS: %d', $frstream->{avg_frame_rate} );

	$formats->{probedDuration}                                                    = $formats->{duration} * 1000 * 1000; ## Probe Duration is set up in microseconds.
	$formats->{probeFPS}                                                          = $frstream->{avg_frame_rate} * 8;
	( $formats->{probedDuration} > $maxProbeDura ) and $formats->{probedDuration} = $maxProbeDura;
	( $formats->{probeFPS} > $maxProbeFPS ) and $formats->{probeFPS}              = $maxProbeFPS;

	$formats->{sourceFPS}    = $frstream->{avg_frame_rate};
	$formats->{probeStrings} = sprintf '-probesize %d -analyzeduration %d -fpsprobesize %d',
	                                   $formats->{probeSize},
	                                   $formats->{probedDuration},
	                                   $formats->{probeFPS};

	# Now that we have good (and sane) values for probing sizes and durations, lets query ffprobe again to get the final value we need.
	can_work() or return 0;
	$stream_fields = 'avg_frame_rate,channels,codec_name,codec_type,nb_streams,pix_fmt,r_frame_rate,stream_type,duration';
	$frstream      = get_info_from_ffprobe( $src, $stream_fields );
	( $frstream_no >= 0 ) or return 0;    ## Something went wrong this time
	$frstream = $streams->[$frstream_no]; ## shortcut, four...

	# Maybe we have to fix the duration values we currently have
	$formats->{duration} =~ m/(\d+[.]\d+)/ms and
	$formats->{duration} = floor( 1. + ( 1. * $1 ));

	# Now we can go through the read stream information and determine video and audio stream details
	analyze_stream_info( $src, $streams ) or return 0;

	# If the second analysis somehow came up with a different average framerate, we have to adapt:
	if ( ( $formats->{duration} > 0 ) && ( $frstream->{avg_frame_rate} > 0 ) &&
	     ( $formats->{sourceFPS} != $formats->{avg_frame_rate} ) ) {
		$formats->{probedDuration} = $formats->{duration} * 1000 * 1000; ## Probe Duration is set up in microseconds.
		$formats->{probeFPS}       = $frstream->{avg_frame_rate} * 8;

		log_debug( '(fixed) Duration   : %d', $formats->{duration} );
		log_debug( '(fixed) Average FPS: %d', $frstream->{avg_frame_rate} );

		( $formats->{probedDuration} > $maxProbeDura ) and $formats->{probedDuration} = $maxProbeDura;
		( $formats->{probeFPS} > $maxProbeFPS ) and $formats->{probeFPS}              = $maxProbeFPS;

		$formats->{sourceFPS} = $frstream->{avg_frame_rate};
	}

	return 1;
}

sub analyze_stream_info {
	my ( $src, $streams ) = @_;

	my $have_video = 0;
	my $have_audio = 0;
	my $have_voice = 0;

	for ( 0 .. ( $source_info{$src}{nb_streams} - 1 ) ) {
		my $i = $_; ## save the magic bullet
		if ( $streams->[$i]{codec_type} eq 'video' ) {
			$have_video   = 1;
			$video_stream = $i;
			$streams->[$i]{avg_frame_rate} =~ m/(\d+)\/(\d+)/ms and ( 1. * $1 > 0. ) and ( 1. * $2 > 0. )
			and $streams->[$i]{avg_frame_rate} = floor( 1. * ( ( 1. * $1 ) / ( 1. * $2 ) ));
		}
		if ( $streams->[$i]{codec_type} eq 'audio' ) {
			if ( 0 == $have_audio ) {
				$have_audio   = 1;
				$audio_stream = $i;
				if ( $streams->[$i]{channels} > $audio_channels ) {
					$audio_channels = $streams->[$i]{channels};
					$audio_layout   = channels_to_layout( $audio_channels );
				}
			} elsif ( 0 == $have_voice ) {
				$have_voice   = 1;
				$voice_stream = $i;
				if ( $streams->[$i]{channels} > $voice_channels ) {
					$voice_channels = $streams->[$i]{channels};
					$voice_layout   = channels_to_layout( $voice_channels );
				}
			} else {
				log_error( "Found third audio channel in '%s' - no idea what to do with it!", $src );
				return 0;
			}
		}
	}
	( 0 == $have_video ) and log_error( "Source file '%s' has no video stream!", $src ) and return 0;

	return 1;
}

sub assemble_output {
	can_work() or return 1;
	my $lstfile = sprintf '%s/temp_%d_src.lst', dirname( $path_target ), $tmp_pid;
	my $prgfile = sprintf '%s/temp_%d_prg.log', dirname( $path_target ), $tmp_pid;
	my $mapfile = $path_target;
	$mapfile =~ s/[.]mkv$/.wav/ms;

	if ( open my $fOut, '>', $lstfile ) {
		foreach my $groupID ( sort { $a <=> $b } keys %source_groups ) {
			for my $i ( 0 .. 3 ) {
				printf { $fOut } "file '%s'\n", ( sprintf $source_groups{$groupID}{idn}, $i );
			}
		}
		close $fOut or croak( "Closing listfile '$lstfile' FAILED!" );
	} else {
		log_error( "Unable to write into '%s': %s", $lstfile, $! );
		exit 11;
	}

	# Having a list file we can go and create our output:
	if ( can_work() ) {
		create_target_file( $lstfile, $prgfile, $mapfile ) or exit 12;
	}

	# When everything is good, we no longer need the list file, progress file and the temp files
	if ( 0 == $do_debug ) {
		-f $lstfile and unlink $lstfile;
		-f $prgfile and unlink $prgfile;
		foreach my $groupID ( sort { $a <=> $b } keys %source_groups ) {
			for my $i ( 0 .. 3 ) {
				my $tmpfile = sprintf $source_groups{$groupID}{idn}, $i;
				log_debug( 'Removing %s...', $tmpfile );
				-f $tmpfile and unlink $tmpfile;
			}
		}
	}

	return 1;
}

sub build_source_groups {
	can_work() or return 1;
	( $source_count > 1 ) or return 0;
	my $group_id    = 0;
	my $last_dir    = ( 0 == ( length $path_temp ) ) ? 'n/a' : $path_temp;
	my $last_ch_cnt = 0;
	my %last_codec  = ();
	my $tmp_count   = 0;

	foreach my $fileID ( sort { $a <=> $b } keys %source_ids ) {
		can_work() or last;
		my $src  = $source_ids{$fileID};
		my $data = $source_info{$src}; ## shortcut

		# The next group is needed, if channel count, any codec or the directory changes.
		my $dir_changed   = ( 0 == ( length $path_temp ) ) && ( $data->{dir} ne $last_dir );
		my $ch_changed    = ( $data->{nb_streams} != $last_ch_cnt );
		my $codec_changed = 0;

		# Codecs must be looked at in a loop, as we do not know how many there are.
		for ( 0 .. ( $data->{nb_streams} - 1 ) ) {
			( ( !defined $last_codec{$_} ) or ( $last_codec{$_} ne $data->{streams}[$_]{codec_name} ) )
			and $codec_changed = 1;
			$last_codec{$_}    = $data->{streams}[$_]{codec_name};
		}
		$last_dir    = ( 0 == ( length $path_temp ) ) ? $data->{dir} : $path_temp;
		$last_ch_cnt = $data->{nb_streams};

		# Let's start a new group if anything changed
		if ( ( $dir_changed + $ch_changed + $codec_changed ) > 0 ) {
			## no critic (ProhibitParensWithBuiltins)
			$source_groups{ ++$group_id } = {
				dir  => $last_dir,
				dur  => 0,
				fps  => 0,
				idn  => sprintf( '%s/temp_%d_inter_dn_%d_%%d.mkv', $last_dir, $tmp_pid, ++$tmp_count ),
				ids  => [],
				iup  => sprintf( '%s/temp_%d_inter_up_%d_%%d.mkv', $last_dir, $tmp_pid, ++$tmp_count ),
				lst  => sprintf( '%s/temp_%d_segments_%d_src.lst', $last_dir, $tmp_pid, ++$tmp_count ),
				prg  => sprintf( '%s/temp_%d_progress_%d_%%d.prg', $last_dir, $tmp_pid, ++$tmp_count ),
				srcs => [],
				tmp  => sprintf( '%s/temp_%d_segments_%d_%%d.mkv', $last_dir, $tmp_pid, ++$tmp_count )
			};
		}

		# Now add the file
		$source_groups{$group_id}{dur} += $data->{duration};
		$data->{sourceFPS} > $source_groups{$group_id}{fps}
		and $source_groups{$group_id}{fps} = $data->{sourceFPS};
		push @{ $source_groups{$group_id}{ids} }, $fileID;
		push @{ $source_groups{$group_id}{srcs} }, $src;
	} ## End of grouping input files

	return 1;
}

sub can_work {
	return 0 == $death_note;
}


# ----------------------------------------------------------------
# Simple Wrapper around IPC::Cmd to capture simple command outputs
# ----------------------------------------------------------------
sub capture_cmd {
	my ( @cmd ) = @_;
	my $kid     = fork;

	defined $kid or croak( "Cannot fork()! $!\n" );

	# Handle being the fork first
	# =======================================
	if ( 0 == $kid ) {
		start_capture( @cmd );
		POSIX::_exit( 0 ); ## Regular exit() would call main::END block
	}

	# === Do the bookkeeping before we wait
	# =======================================
	add_pid( $kid );

	# Wait on the fork to finish
	# =======================================
	wait_for_capture( $kid );

	# Handle result:
	$work_lock->lock;
	if ( 0 != $work_data->{PIDs}{$kid}{exit_code} ) {
		log_error( q{Command '%s' FAILED [%d] : %s}, join( $SPACE, @cmd ), $work_data->{PIDs}{$kid}{exit_code}, $work_data->{PIDs}{$kid}{result} );
		$work_lock->unlock;
		croak( 'capture_cmd() crashed' );
	}

	my $result = $work_data->{PIDs}{$kid}{result};
	$work_lock->unlock;

	remove_pid( $kid, 1 );

	return $result;
}

sub channels_to_layout {
	my ( $channels ) = @_;
	( 1 == $channels ) and return 'mono';
	( 2 == $channels ) and return 'stereo';
	( 3 == $channels ) and return '2.1';
	( 4 == $channels ) and return 'quad';
	( 5 == $channels ) and return '4.1';
	( 6 == $channels ) and return '5.1';
	( 7 == $channels ) and return '6.1';
	( 8 == $channels ) and return '7.1';
	return 'guess';
}

sub check_arguments {
	my $errCount    = 0;
	my $have_source = 0;
	my $have_target = 0;
	my $total_size  = 0;

	check_source_and_target( \$errCount, \$have_source, \$have_target );
	$have_source and check_input_files( \$errCount, \$total_size );
	$have_target and check_output_existence( \$errCount ) and $have_source and check_temp_dir( \$errCount, $total_size );

	return $errCount;
}

sub check_input_files {
	my ( $errCount, $total_size ) = @_;
	foreach my $src ( @path_source ) {
		validate_input_file( $src, $total_size ) or ${ $errCount }++;
	}
	return 1;
}

sub check_output_existence {
	my ( $errCount ) = @_;

	-f $path_target and log_error( 'Output file already exists!', $path_target ) and ++${ $errCount };
	foreach my $src ( @path_source ) {
		$src eq $path_target and log_error( 'Input file equals output file!', $src ) and ++${ $errCount };
		$path_target =~ m/[.]mkv$/ms or log_error( 'Output file does not have mkv ending!' ) and ++${ $errCount };
	}

	return 1;
}

sub check_source_and_target {
	my ( $errCount, $have_source, $have_target ) = @_;

	${ $have_source } = scalar @path_source;
	${ $have_target } = length $path_target;

	${ $have_target } and set_log_file() and ${ $have_source } and return 1;

	${ $have_source } or log_error( 'No Input given!' ) and ++${ $errCount };
	${ $have_target } or log_error( 'No Output given!' ) and ++${ $errCount };

	return 0;
}

sub check_multi_temp_dir {
	my ( $errCount ) = @_;

	foreach my $src ( @path_source ) {
		if ( -f $src ) {
			my $dir                     = dirname( $src );
			( length $dir ) > 0 or $dir = q{.};
			if ( !defined $dir_stats{$dir} ) {
				$dir_stats{$dir} = { has_space => 0, need_space => 0, srcs => [] };
			}
			push @{ $dir_stats{$dir}{srcs} }, $src;
			my $ref = df( $dir );
			if ( defined $ref ) {
				$dir_stats{$dir}{has_space}  += $ref->{bavail} / 1024;     ## again count in M not K
				$dir_stats{$dir}{need_space} += ( -s $src ) / 1024 / 1024; ## also in M now.
			} else {
				# =) df() failed? WTF?
				log_error( "df'ing directory '%s' FAILED!", $dir ) and ++${ $errCount };
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
		and ++${ $errCount };
	}

	return 1;
}

sub check_single_temp_dir {
	my ( $errCount, $total_size ) = @_;

	if ( -d $path_temp ) {
		# =) Temp Dir exists
		my $ref                = df( $path_temp );
		$dir_stats{$path_temp} = { has_space => 0, need_space => 0, srcs => [] };
		foreach my $src ( @path_source ) {
			push @{ $dir_stats{$path_temp}{srcs} }, $src;
		}
		if ( defined $ref ) {
			# The temporary UT Video files will need roughly 42-47 times the input
			# Plus a probably 3 times bigger output than input and we end at x50.
			my $needed_space = $total_size * 50;
			my $have_space   = $ref->{bavail} / 1024; # df returns 1K blocks, but we calculate in M.
			if ( $have_space < $needed_space ) {
				log_error( "Not enough space! '%s' has only %s / %s M free!",
				           $path_temp,
				           cleanint( $have_space ),
				           cleanint( $needed_space )) and ++${ $errCount };
			}
		} else {
			# =) df() failed? WTH?
			log_error( "df'ing directory '%s' FAILED!", $path_temp ) and ++${ $errCount };
		}
	} else {
		# =) Temp Dir does NOT exist
		log_error( "Temp directory '%s' does not exist!", $path_temp ) and ++${ $errCount };
	}

	return 1;
}

# Make sure we have a sane target FPS. max_fps is reused as upper fps
sub check_target_fps {
	can_work() or return 1;
	$target_fps = ( ( $max_fps < 50 ) && ( 0 == $force_upgrade ) ) ? 30 : 60;
	$max_fps    = 2 * $target_fps;
	log_info( 'Decimate and interpolate up to %d FPS', $max_fps );
	log_info( 'Then interpolate to the target %d FPS', $target_fps );
	return 1;
}

sub check_temp_dir {
	my ( $errCount, $total_size ) = @_;

	return ( ( length $path_temp ) > 0 ) ? check_single_temp_dir( $errCount, $total_size ) : check_multi_temp_dir( $errCount );
}

sub set_log_file {
	$logfile = $path_target;
	$logfile =~ s/[.][^.]+$/.log/ms;
	return 1;
}

sub validate_input_file {
	my ( $src, $total_size ) = @_;
	if ( -f $src ) {
		my $in_size = -s $src;
		( $in_size > 0 ) or log_error( "Input file '%s' is empty!", $src ) and return 0;
		${ $total_size } += $in_size / 1024 / 1024; # We count 1M blocks
		++$source_count;
	} else {
		log_error( "Input file '%s' does not exist!", $src );
		return 0;
	}
	return 1;
}

sub cleanint {
	my ( $float ) = @_;
	my $int       = floor( $float );
	return commify( $int );
}

sub close_standard_io {
	my $devnull = '/dev/null';
	## no critic (RequireCheckedClose, RequireCheckedSyscalls)
	close STDIN and open STDIN, '<', $devnull;
	close STDOUT and open STDOUT, '>', $devnull;
	close STDERR and open STDERR, '>', $devnull;
	return 1;
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
		@mapVoice = ( '-map', "0:$voice_stream", qw( -vn -codec:a:0 pcm_s24le ) );
		( 2 != $voice_channels ) and push @mapVoice, qw( -channel_layout:a:0 stereo -ac:a:0 2 );
		push @mapVoice, $mapfile;
	}

	# The main audio is probably in 7.1 and we need 5.1 in channel 0 and stereo in channel 1
	my @mapAudio  = ( qw( -map 0:0 -map ), "0:$audio_stream", qw( -codec:a:0 pcm_s24le ) );
	my @metaAudio = qw( -map_metadata 0 -metadata:s:a:0 title=Stereo -metadata:s:a:0 language=eng );

	if ( 2 < $audio_channels ) {
		push @mapAudio, ( qw( -channel_layout:a:0 5.1 -ac:a:0 6 -map ), "0:$audio_stream", qw( -codec:a:1 pcm_s24le -channel_layout:a:1 stereo -ac:a:1 2 ) );
		@metaAudio = qw( -map_metadata 0 -metadata:s:a:0 title=Surround -metadata:s:a:0 language=eng -metadata:s:a:1 title=Stereo -metadata:s:a:1 language=eng );
	}

	# Building the four worker threads is quite trivial
	can_work() or return 1;
	my @ffargs = ( $FF, @FF_ARGS_START, '-progress', $prgfile,
	               ( ( 'guess' ne $audio_layout ) ? qw( -guess_layout_max 0 ) : () ),
	               @FF_ARGS_INPUT_CUDA, $lstfile,
	               @mapAudio, @metaAudio, @FF_ARGS_FILTER, $F_assembled, '-fps_mode', 'vfr', @FF_ARGS_FORMAT,
	               @FF_ARGS_CODEC_h264, $path_target, @mapVoice );

	log_debug( "Starting Worker 1 for:\n%s", ( join $SPACE, @ffargs ));
	my $pid = start_work( 1, @ffargs );
	defined $pid and ( $pid > 0 ) or confess( 'BUG! start_work() returned invalid PID!' );
	$work_lock->lock;
	$work_data->{PIDs}{$pid}{args}    = \@ffargs;
	$work_data->{PIDs}{$pid}{prgfile} = $prgfile;
	$work_lock->unlock;

	# Watch and join
	return watch_my_forks();
}

sub commify {
	my ( $text ) = @_;
	$text        = reverse $text;
	$text =~ s/(\d\d\d)(?=\d)(?!\d*[.])/$1,/gxms;
	return scalar reverse $text;
}

sub declare_single_source {
	can_work() or return 1;
	( 1 == $source_count ) or return 0;
	my $src      = $path_source[0];
	my $data     = $source_info{$src}; ## shortcut
	my $fileID   = $data->{id};
	my $last_dir = ( 0 == ( length $path_temp ) ) ? $data->{dir} : $path_temp;

	## no critic (ProhibitParensWithBuiltins)
	$source_groups{ 0 } = {
		dir  => $last_dir,
		dur  => $data->{duration},
		fps  => $data->{sourceFPS},
		idn  => sprintf( '%s/temp_%d_inter_dn_%d_%%d.mkv', $last_dir, $tmp_pid, 1 ),
		ids  => [ $fileID ],
		iup  => sprintf( '%s/temp_%d_inter_up_%d_%%d.mkv', $last_dir, $tmp_pid, 2 ),
		lst  => sprintf( '%s/temp_%d_segments_%d_src.lst', $last_dir, $tmp_pid, 3 ),
		prg  => sprintf( '%s/temp_%d_progress_%d_%%d.prg', $last_dir, $tmp_pid, 4 ),
		srcs => [ $src ],
		tmp  => sprintf( '%s/temp_%d_segments_%d_%%d.mkv', $last_dir, $tmp_pid, 5 )
	};

	return 1;
}


# A die handler that lets perl death notes be printed via log
sub dieHandler {
	my ( $err ) = @_;
	$ret_global = 42;
	return log_error( '%s', $err );
}

sub file_exists {
	my ( $file ) = @_;
	return defined $file && ( ( length $file ) > 0 ) && ( -f $file );
}

sub format_bitrate {
	my ( $float ) = @_;
	return lc( human_readable_size( $float )) . 'bits/s';
}

sub format_out_time {
	my ( $ms ) = @_;
	my $sec    = floor( $ms / 1_000_000 );
	my $min    = floor( $sec / 60 );
	my $hr     = floor( $min / 60 );

	return sprintf '%02d:%02d:%02d.%06d', $hr, $min % 60, $sec % 60, $ms % 1_000_000;
}

sub get_info_from_ffprobe {
	my ( $src, $stream_fields ) = @_;
	my $avg_frame_rate_stream   = -1;
	my @fpcmd                   = ( $FP,
	                                @FP_ARGS,
	                                "stream=$stream_fields",
	                                split( $SPACE, $source_info{$src}{probeStrings} ),
	                                $src );

	log_debug( 'Calling: %s', ( join $SPACE, @fpcmd ));

	my @fplines = split /\n/ms, capture_cmd( @fpcmd );

	foreach my $line ( @fplines ) {
		chomp $line;
		#log_debug("RAW[%s]", $line);
		if ( $line =~ m/streams_stream_(\d)_([^=]+)="?([^"]+)"?/xms ) {
			$source_info{$src}{streams}[$1]{$2} = "$3";
			( 'avg_frame_rate' eq $2 ) and
			( '0' ne "$3" ) and ( '0/0' ne "$3" ) and
			( $avg_frame_rate_stream < 0 ) and $avg_frame_rate_stream = $1;
			next;
		}
		if ( $line =~ m/format_([^=]+)="?([^"]+)"?/xms ) {
			$source_info{$src}{$1} = "$2";
		}
	}

	return $avg_frame_rate_stream;
}

sub get_location {
	my ( $caller, undef, undef, $lineno, $logger ) = @_;

	defined $logger and $logger =~ m/^main::log_(info|warning|error|status|debug)$/xms
	or confess( "get_location(): logMsg() called from wrong sub $logger" );

	my $subname = $caller // 'main';
	$subname =~ s/^.*::([^:]+)$/$1/xms;
	defined $lineno or $lineno = -1;

	return ( $lineno > -1 ) ? ( sprintf '%d:%s()', $lineno, $subname ) : ( sprintf '%s', $subname );
}

sub get_log_level {
	my ( $level ) = @_;

	( $LOG_INFO == $level ) and return ( 'Info   ' ) or
	( $LOG_WARNING == $level ) and return ( 'Warning' ) or
	( $LOG_ERROR == $level ) and return ( 'ERROR  ' ) or
	( $LOG_STATUS == $level ) and return ( $EMPTY );

	return ( '=DEBUG=' );
}

sub get_pid_status {
	my ( $pid ) = @_;
	$work_lock->lock;
	my $status = $work_data->{PIDs}{$pid}{status};
	$work_lock->unlock;
	return $status;
}

sub get_time_now {
	my @tLocalTime = localtime;
	return sprintf '%04d-%02d-%02d %02d:%02d:%02d',
	               $tLocalTime[5] + 1900, $tLocalTime[4] + 1, $tLocalTime[3],
	               $tLocalTime[2], $tLocalTime[1], $tLocalTime[0];
}

sub handle_fork_progress {
	my ( $pid, $prgData, $fork_timeout, $fork_strikes ) = @_;
	my $result                                          = 1;

	defined( $fork_timeout->{$pid} ) or $fork_timeout->{$pid} = $TIMEOUT_INTERVALS;
	defined( $fork_strikes->{$pid} ) or $fork_strikes->{$pid} = 0;

	reap_pid( $pid ) and $result = 0; # the PID will give no progress any more

	# Check/Initialize the progress hash
	defined( $prgData->{bitrate} ) or $prgData->{bitrate}         = 0.0; ## "0.0kbits/s" in the file
	defined( $prgData->{drop_frames} ) or $prgData->{drop_frames} = 0;
	defined( $prgData->{dup_frames} ) or $prgData->{dup_frames}   = 0;
	defined( $prgData->{fps} ) or $prgData->{fps}                 = 0.0;
	defined( $prgData->{frames} ) or $prgData->{frames}           = 0;
	defined( $prgData->{out_time} ) or $prgData->{out_time}       = 0; ## "00:00:00.000000" in the file, but we read out_time_ms
	defined( $prgData->{total_size} ) or $prgData->{total_size}   = 0;

	load_progress( $work_data->{PIDs}{$pid}{prgfile}, $prgData ) and $fork_timeout->{$pid} = $TIMEOUT_INTERVALS or --$fork_timeout->{$pid};

	return $result
}

sub handle_fork_strikes {
	my ( $pid, $fork_timeout, $fork_strikes ) = @_;

	my $status                                                = get_pid_status( $pid );
	defined( $fork_timeout->{$pid} ) or $fork_timeout->{$pid} = $TIMEOUT_INTERVALS;
	defined( $fork_strikes->{$pid} ) or $fork_strikes->{$pid} = 0;

	if ( ( $fork_strikes->{$pid} <= 0 ) and ( $FF_RUNNING == $status ) ) {
		++$fork_strikes->{$pid};
		$fork_strikes->{$pid} = ( $fork_strikes->{$pid} == 13 ) ? strike_fork_reap( $pid ) :
		                        ( $fork_strikes->{$pid} == 7 ) ? strike_fork_kill( $pid ) :
		                        ( $fork_strikes->{$pid} == 1 ) ? strike_fork_term( $pid ) :
		                        $fork_strikes->{$pid};
		if ( $fork_strikes->{$pid} > 17 ) {
			my $kid               = strike_fork_restart( $pid );
			$fork_timeout->{$kid} = $TIMEOUT_INTERVALS;
			$fork_strikes->{$kid} = 0;
			log_debug( 'Worker PID %d substituted PID %d', $kid, $pid );
		}
	}

	return 1;
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

	return sprintf '%3.2f%s', floor( $int * 100. ) / 100., $exps[$exp];
}

sub interpolate_source_group {
	my ( $gid, $tmp_from, $tmp_to, $filter_string ) = @_;
	can_work() or return 1;

	if ( !defined $source_groups{$gid} ) {
		log_error( 'Source Group ID %d does not exist!', $gid );
		return 0;
	}

	defined $filter_string or $filter_string = $EMPTY;
	can_work() or return 1;

	# Building the four worker threads is quite trivial
	can_work() or return 1;
	for ( 0 .. 3 ) {
		start_worker_fork( $gid, $_, $tmp_from, $tmp_to, $filter_string ) or return 0;
	}

	# Watch and join
	return watch_my_forks();
}

sub is_progress_line {
	my ( $line ) = @_;
	return $line =~ m/^progress=/xms;
}


# Load data from between the last two "progress=<state>" lines in the given log file, and store it in the given hash
# If the hash has values, progress data is added.
sub load_progress {
	my ( $progress_log, $progress_data ) = @_;

	file_exists( $progress_log ) or croak( "Log file $progress_log does not exist!" );

	my @args          = ( 'tail', '-n', '20', $progress_log );
	my @last_20_lines = reverse split /\n/ms, capture_cmd( @args );
	my $lines_count   = scalar @last_20_lines;

	my $progress_count = 0;
	my $i              = 0;
	while ( ( $progress_count < 1 ) && ( $i < $lines_count ) ) {
		is_progress_line( $last_20_lines[$i] ) and $progress_count++;
		$i++;
	}

	my @progress_field_names = qw( bitrate drop_frames dup_frames fps frame out_time_ms total_size );
	while ( ( $progress_count < 2 ) && ( $i < $lines_count ) ) {
		if ( is_progress_line( $last_20_lines[$i] ) ) {
			$progress_count++;
		} else {
			foreach ( @progress_field_names ) {
				parse_progress_data( $last_20_lines[$i], $_, $progress_data ) && $i++;
			}
		}
		$i++;
	}
	return $progress_count == 2 ? 1 : 0;
}

sub logMsg {
	my ( $lvl, $fmt, @args ) = @_;

	defined $lvl or $lvl = 2;

	( $LOG_DEBUG == $lvl ) and ( 0 == $do_debug ) and return 1;

	my $stTime  = get_time_now();
	my $stLevel = get_log_level( $lvl );
	my $stLoc   = get_location( ( caller 2 )[3], caller 1 );
	my $stMsg   = sprintf "%s|%s|%s|$fmt", $stTime, $stLevel, $stLoc, @args;

	( 0 < ( length $logfile ) ) and write_to_log( $stMsg );
	( $LOG_DEBUG != $lvl ) and write_to_console( $stMsg );

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


sub make_filter_string {
	my ( $tmp_to, $dec_max, $dec_frac, $tgt_fps ) = @_;
	can_work() or return 1;

	# Prepare filter components
	my $F_in_scale    = "scale='in_range=full:out_range=full'";
	my $F_scale_FPS   = "fps=${tgt_fps}:round=near";
	my $F_mpdecimate  = "mpdecimate='max=${dec_max}:frac=${dec_frac}'";
	my $F_out_scale   = "scale='flags=accurate_rnd+full_chroma_inp+full_chroma_int:in_range=full:out_range=full'";
	my $F_interpolate = "libplacebo='extra_opts=preset=high_quality:frame_mixer=" .
	                    ( ( 'iup' eq $tmp_to ) ? 'mitchell_clamp' : 'oversample' ) .
	                    ":fps=${tgt_fps}'";
	return "${B_in}${F_in_scale}" .
	       ( ( 'iup' eq $tmp_to ) ? "${B_FPS}${F_scale_FPS}" : $EMPTY ) .
	       "${B_decimate}${F_mpdecimate}${B_middle}${F_out_scale}${B_interp}${F_interpolate}${B_out}";
}

sub parse_progress_data {
	my ( $line, $property_name, $data ) = @_;
	if ( $line =~ m/^${property_name}=(\S+)\s*$/xms ) {
		# Assuming all metadata are numeric values
		$data->{$property_name} += ( 1 * $1 );
		return 1;
	}
	return 0;
}

sub pid_exists {
	my ( $pid ) = @_;
	$work_lock->lock;
	my $exists = defined( $work_data->{PIDs}{$pid} );
	$work_lock->unlock;
	return $exists;
}

sub reap_pid {
	my ( $pid ) = @_;

	defined $pid and ( $pid =~ m/^\d+$/ms )
	or log_error( q{reap_pid(): BUG! '%s' is not a valid pid!}, $pid // 'undef' ) and confess( 'FATAL BUG!' );
	defined( $work_data->{PIDs}{$pid} ) or return 1;
	$work_lock->lock;
	my $status = $work_data->{PIDs}{$pid}{status} // $FF_REAPED;
	$work_lock->unlock;
	( $FF_REAPED == $status ) and return 1;

	( 0 == ( waitpid $pid, POSIX::WNOHANG ) ) and return 0; ## PID is still busy!

	log_debug( '(reap_pid) PID %d finished', $pid );

	$work_lock->lock;
	$work_data->{PIDs}{$pid}{status} = $FF_REAPED;
	$work_lock->unlock;

	return 1;
}


# ---------------------------------------------------------
# REAPER function - $SIG{CHLD} exit handler
# ---------------------------------------------------------
sub reaper {
	my @args = @_;

	while ( ( my $pid = ( waitpid -1, POSIX::WNOHANG ) ) > 0 ) {
		log_debug( '(reaper) KID %d finished [%s]', $pid, ( join q{,}, @args ));
		$work_lock->lock;
		$work_data->{PIDs}{$pid}{status} = $FF_REAPED;
		$work_lock->unlock;
	}

	$SIG{CHLD} = \&reaper;

	return 1;
}

sub remove_pid {
	my ( $pid, $do_cleanup ) = @_;
	defined $pid and ( $pid =~ m/^\d+$/ms )
	or log_error( "remove_pid(): BUG! '%s' is not a valid pid!", $pid // 'undef' ) and confess( 'FATAL BUG!' );
	defined $work_data->{PIDs}{$pid} or return 1;

	my $result = 1;

	while ( can_work && ( 0 == reap_pid( $pid ) ) ) {
		usleep( 50 );
	}

	$work_lock->lock;

	# If we shall clean up source and maybe target files, do so now
	if ( 1 == $do_cleanup ) {
		log_debug( 'args      => %d', scalar @{ $work_data->{PIDs}{$pid}{args} // [] } );
		log_debug( 'exit_code => %d', $work_data->{PIDs}{$pid}{exit_code} // -1 );
		log_debug( 'id        => %d', $work_data->{PIDs}{$pid}{id} // -1 );
		log_debug( 'prgfile   => %s', $work_data->{PIDs}{$pid}{prgfile} // 'undef' );
		log_debug( 'result    => %s', $work_data->{PIDs}{$pid}{result} // 'undef' );
		log_debug( 'source    => %s', $work_data->{PIDs}{$pid}{source} // 'undef' );
		log_debug( 'target    => %s', $work_data->{PIDs}{$pid}{target} // 'undef' );
		if ( defined( $work_data->{PIDs}{$pid}{exit_code} ) && ( $work_data->{PIDs}{$pid}{exit_code} != 0 ) ) {
			log_error( 'Worker PID %d FAILED [%d] %s', $pid, $work_data->{PIDs}{$pid}{exit_code}, $work_data->{PIDs}{$pid}{result} );
			# We do not need the target file any more, the thread failed! (if an fmt is set)
			if ( 0 == $do_debug ) {
				if ( 0 < length( $work_data->{PIDs}{$pid}{target} ) ) {
					my $f = sprintf $work_data->{PIDs}{$pid}{target}, $work_data->{PIDs}{$pid}{id};
					log_debug( "Removing target file '%s' ...", $f );
					-f $f and unlink $f;
				}
			}
			$result = 0; ## We _did_ fail!
		}

		# We do not need the source file any more (if an fmt is set)
		if ( defined( $work_data->{PIDs}{$pid}{source} ) && ( 0 == $do_debug ) ) {
			if ( 0 < length( $work_data->{PIDs}{$pid}{source} ) ) {
				my $f = sprintf $work_data->{PIDs}{$pid}{source}, $work_data->{PIDs}{$pid}{id};
				log_debug( "Removing source file '%s' ...", $f );
				-f $f and unlink $f;
			}
		}
	}

	# Progress files are already removed, because the only part where they are used
	# will no longer pick them up once the PID was removed from %work_data (See watch_my_forks())
	my $prgfile = $work_data->{PIDs}{$pid}{prgfile} // $EMPTY; ## shortcut including defined check
	( length( $prgfile ) > 0 ) and ( -f $prgfile ) and unlink $prgfile;

	delete( $work_data->{PIDs}{$pid} );
	--$work_data->{cnt};

	$work_lock->unlock;

	return $result;
}


# ---------------------------------------------------------
# A signal handler that sets global vars according to the
# signal given.
# Unknown signals are ignored.
# ---------------------------------------------------------
sub sigHandler {
	my ( $sig ) = @_;
	if ( exists $signalHandlers{$sig} ) {
		$signalHandlers{$sig}->();
	} else {
		log_warning( 'Caught Unknown Signal [%s] ... ignoring Signal!', $sig );
	}
	return 1;
}

sub segment_all_groups {
	can_work() or return 1;

	log_info( 'Segmenting source groups...' );

	foreach my $groupID ( sort { $a <=> $b } keys %source_groups ) {
		can_work() or last;
		my $prgfile = sprintf '%s/temp_%d_progress_%d.log', $source_groups{$groupID}{dir}, $tmp_pid, $groupID;
		segment_source_group( $groupID, $prgfile ) or exit 8;
		-f $prgfile and ( 0 == $do_debug ) and unlink $prgfile;
	}

	return 1;
}

sub segment_source_group {
	my ( $gid, $prgfile ) = @_;
	defined $source_groups{$gid} or log_error( 'Source Group ID %d does not exist!', $gid ) and return 0;
	can_work() or return 1;

	# We use this to check on the overall maximum fps
	( $source_groups{$gid}{fps} > $max_fps ) and $max_fps = $source_groups{$gid}{fps};

	# Each segment must be a quarter of the total duration, raised to the next full second
	my $seg_len = floor( 1. + ( $source_groups{$gid}{dur} / 4. ));

	# Luckily we can concat and segment in one go, but we need the concat demuxer for that, which requires an input file
	if ( open my $fOut, '>', $source_groups{$gid}{lst} ) {
		foreach my $fid ( sort { $a <=> $b } @{ $source_groups{$gid}{ids} } ) {
			printf { $fOut } "file '%s'\n", $source_ids{$fid};
		}
		close $fOut or confess( "Closing listfile '$source_groups{$gid}{lst}' FAILED!" );
	} else {
		log_error( q{Cannot write list file '%s': %s}, $source_groups{$gid}{lst}, $! );
		return 0;
	}

	# Let's build the command line arguments:
	my @ffargs = ( $FF, @FF_ARGS_START, '-progress', $prgfile,
	               ( ( 'guess' ne $audio_layout ) ? qw( -guess_layout_max 0 ) : () ),
	               @FF_CONCAT_BEGIN, $source_groups{$gid}{lst}, @FF_CONCAT_END,
	               qw( -f segment -segment_time ), "$seg_len",
	               $source_groups{$gid}{tmp} );

	log_debug( "Starting Worker %d for:\n%s", 1, ( join $SPACE, @ffargs ));
	my $pid = start_work( 1, @ffargs );
	defined $pid and ( $pid > 0 ) or croak( 'BUG! start_work() returned invalid PID!' );
	$work_lock->lock;
	$work_data->{PIDs}{$pid}{args}    = \@ffargs;
	$work_data->{PIDs}{$pid}{prgfile} = $prgfile;
	$work_lock->unlock;

	# Watch and join
	my $result = watch_my_forks();

	# The list file is no longer needed.
	-f $source_groups{$gid}{lst} and ( 0 == $do_debug ) and unlink $source_groups{$gid}{lst};

	return $result;
}

sub send_forks_the_kill() {
	$work_lock->lock;
	foreach my $pid ( keys %{ $work_data->{PIDs} } ) {
		if ( ( 1 == $death_note ) && ( $work_data->{PIDs}{$pid}{status} != $FF_REAPED ) ) {
			log_warning( 'TERMing worker PID %d', $pid );
			$work_lock->unlock;
			terminator( $pid, 'TERM' );
			$work_lock->lock;
		}
		# Note: 5 is after 2 seconds
		elsif ( ( 5 == $death_note ) && ( $work_data->{PIDs}{$pid}{status} != $FF_REAPED ) ) {
			log_warning( 'KILLing worker PID %d', $pid );
			$work_lock->unlock;
			terminator( $pid, 'KILL' );
			$work_lock->lock;
		}
	}
	++$death_note;
	$work_lock->unlock;
	return 1;
}


# Show data from between the last two "progress=<state>" lines in the given log file
sub show_progress {
	my ( $thr_count, $thr_active, $prgData, $log_as_info ) = @_;

	# Formualate the progress line
	my $size_str     = human_readable_size( $prgData->{total_size} // 0 );
	my $time_str     = format_out_time( $prgData->{out_time} // 0 );
	my $bitrate_str  = format_bitrate( ( $prgData->{bitrate} // 0.0 ) / $thr_count ); ## Average, not the sum.
	my $progress_str = sprintf '[%d/%d running] Frame %d (%d drp, %d dup); %s; FPS: %03.2f; %s; File Size: %s    ',
	                           $thr_active, $thr_count,
	                           $prgData->{frames}, $prgData->{drop_frames}, $prgData->{dup_frames},
	                           $time_str, $prgData->{fps}, $bitrate_str, $size_str;

	# Clear a previous progress line
	( $have_progress_msg > 0 ) and print "\r" . ( $SPACE x length $progress_str ) . "\r";

	if ( 0 < $log_as_info ) {
		# Write into log file
		log_info( '%s', $progress_str );
	} else {
		# Output on console
		$have_progress_msg = 1;
		local $|           = 1;
		print "\r${progress_str}";
	}

	return 1;
}

sub start_capture {
	my @cmd = @_;
	my $pid = $$;

	close_standard_io();

	#@type IPC::Shareable
	my $fork_data = IPC::Shareable->new( key => 'WORK_DATA' );
	#@type IPC::Shareable
	my $fork_lock = tied %{ $fork_data };

	# Wait until we can really start
	wait_for_startup( $pid, $fork_data, $fork_lock );

	# Now we can run the command as desired
	my @stdout;
	run3( \@cmd, \undef, \@stdout, \&log_error, { return_if_system_error => 1 } );

	# We only have to "transport" the results:
	$fork_lock->lock;
	chomp @stdout;
	$fork_data->{PIDs}{$pid}{result}    = join "\n", @stdout;
	$fork_data->{PIDs}{$pid}{exit_code} = 0 + ( $? == -1 ? $! : $? );
	$fork_data->{PIDs}{$pid}{status}    = $FF_FINISHED;
	$fork_lock->unlock;

	return 1;
}

sub start_forked {
	my @cmd = @_;
	my $pid = $$;

	close_standard_io();

	#@type IPC::Shareable
	my $fork_data = IPC::Shareable->new( key => 'WORK_DATA' );
	#@type IPC::Shareable
	my $fork_lock = tied %{ $fork_data };

	# Wait until we can really start
	wait_for_startup( $pid, $fork_data, $fork_lock );

	# Now we can run the command as desired
	my @stderr;
	run3( \@cmd, \undef, \undef, \@stderr, { return_if_system_error => 1 } );

	# We only have to "transport" the results:
	$fork_lock->lock;
	chomp @stderr;
	$fork_data->{PIDs}{$pid}{result}    = join "\n", @stderr;
	$fork_data->{PIDs}{$pid}{exit_code} = 0 + ( $? == -1 ? $! : $? );
	$fork_data->{PIDs}{$pid}{status}    = $FF_FINISHED;
	$fork_lock->unlock;

	return 1;
}


# ---------------------------------------------------------
# Start a command asynchronously
# ---------------------------------------------------------
sub start_work {
	my ( $tid, @cmd ) = @_;
	my $kid           = fork;
	defined $kid or croak( "Cannot fork()! $!\n" );

	# Handle being the fork first
	# =======================================
	if ( 0 == $kid ) {
		start_forked( @cmd );
		POSIX::_exit( 0 ); ## Regular exit() would call main::END block
	}

	# === Do the bookkeeping before we return
	# =======================================
	add_pid( $kid );

	usleep( 0 ); ## Simulates a yield() without multi-threading (We have (a) fork(s) !)
	$work_lock->lock;
	$work_data->{PIDs}{$kid}{status} = $FF_RUNNING;
	log_info( 'Worker %d forked out as PID %d', $tid, $kid );
	$work_lock->unlock;

	return $kid;
}

sub start_worker_fork {
	my ( $gid, $i, $tmp_from, $tmp_to, $F_assembled ) = @_;

	my $prgLog    = sprintf $source_groups{$gid}{prg}, $i;
	my $file_from = sprintf $source_groups{$gid}{$tmp_from}, $i;
	my $file_to   = sprintf $source_groups{$gid}{$tmp_to}, $i;
	my $ffargs    = [ $FF, @FF_ARGS_START, '-progress', $prgLog,
	                  ( ( 'guess' ne $audio_layout ) ? qw( -guess_layout_max 0 ) : () ),
	                  @FF_ARGS_INPUT_VULK, $file_from,
	                  @FF_ARGS_ACOPY_FIL, $F_assembled, '-fps_mode', 'cfr', @FF_ARGS_FORMAT, @FF_ARGS_CODEC_UTV,
	                  $file_to
	];

	log_debug( "Starting Worker %d for:\n%s", $i + 1, ( join $SPACE, @{ $ffargs } ));
	my $pid = start_work( $i, @{ $ffargs } );
	defined $pid and ( $pid > 0 ) or croak( 'BUG! start_work() returned invalid PID!' );
	$work_lock->lock;
	$work_data->{PIDs}{$pid}{args}    = $ffargs;
	$work_data->{PIDs}{$pid}{id}      = $i;
	$work_data->{PIDs}{$pid}{prgfile} = $prgLog;
	$work_data->{PIDs}{$pid}{source}  = $source_groups{$gid}{$tmp_from};
	$work_data->{PIDs}{$pid}{target}  = $source_groups{$gid}{$tmp_to};
	$work_lock->unlock;

	return 1;
}


# --- Second strike, 3 seconds after $thr_progress timeout  ---
# -------------------------------------------------------------
sub strike_fork_kill {
	my ( $pid ) = @_;
	$work_lock->lock;
	if ( $work_data->{PIDs}{$pid}{status} == $FF_RUNNING ) {
		log_warning( 'Sending SIGKILL to fork PID %d', $pid );
		$work_lock->unlock;
		terminator( $pid, 'KILL' );
		return 7;
	}
	log_error( 'Fork PID %d is gone! Will restart...', $pid );
	$work_lock->unlock;
	return 13; # Thread is already gone, start a new one.
}


# --- Third strike, 6 seconds after $thr_progress timeout  ---
# -------------------------------------------------------------
sub strike_fork_reap {
	my ( $pid ) = @_;

	while ( can_work && ( 0 == reap_pid( $pid ) ) ) {
		usleep( 50 );
	}

	return 13;
}


# --- Last strike, 9 seconds after $thr_progress timeout  ---
# -------------------------------------------------------------
sub strike_fork_restart {
	my ( $pid ) = @_;
	log_warning( 'Re-starting frozen Fork %d', $pid );

	$work_lock->lock;
	my @args = @{ $work_data->{PIDs}{$pid}{args} };
	$work_lock->unlock;

	my $kid = start_work( 1, @args );
	defined $kid and ( $kid > 0 ) or croak( 'BUG! start_work() returned invalid PID!' );
	$work_lock->lock;
	$work_data->{PIDs}{$kid}{args}    = $work_data->{PIDs}{$pid}{args};
	$work_data->{PIDs}{$kid}{prgfile} = $work_data->{PIDs}{$pid}{prgfile};
	$work_data->{PIDs}{$kid}{source}  = $work_data->{PIDs}{$pid}{source};
	$work_data->{PIDs}{$kid}{target}  = $work_data->{PIDs}{$pid}{target};
	$work_lock->unlock;

	remove_pid( $pid, 0 ); ## No cleanup, the restarted process will overwrite and we don't want to interfere with the substitute

	return $kid;
}


# --- First strike after $thr_progress ran out (30 seconds) ---
# -------------------------------------------------------------
sub strike_fork_term {
	my ( $pid ) = @_;
	$work_lock->lock;
	if ( $work_data->{PIDs}{$pid}{status} == $FF_RUNNING ) {
		log_warning( 'Sending SIGTERM to frozen fork PID %d', $pid );
		$work_lock->unlock;
		terminator( $pid, 'TERM' );
		return 1;
	}
	log_error( 'Fork PID %d is gone! Will restart...', $pid );
	$work_lock->unlock;
	return 13; # Thread is already gone, start a new one.
}


# ---------------------------------------------------------
# TERM/KILL handler for sending signals to forked children
# ---------------------------------------------------------
sub terminator {
	my ( $kid, $signal ) = @_;

	defined $kid or log_error( 'BUG! terminator() called with UNDEF kid argument!' ) and return 0;
	defined $signal or log_error( 'BUG! terminator() called with UNDEF signal argument!' ) and return 0;

	if ( !( ( 'TERM' eq $signal ) || ( 'KILL' eq $signal ) ) ) {
		log_error( q{Bug: terminator(%d, '%s') called, only TERM and KILL supported!}, $kid, $signal );
		return 0;
	}

	if ( ( $kid ) > 0 ) {
		log_warning( 'Sending %s to pid %d', $signal, $kid );
		if ( kill( $signal, $kid ) > 0 ) {
			usleep( 100_000 );
			reap_pid( $kid );
		} else {
			$work_lock->lock;
			$work_data->{PIDs}{$kid}{status} = $FF_REAPED;
			$work_lock->unlock;
		}
	} else {
		foreach my $pid ( keys %{ $work_data->{PIDs} } ) {
			$work_lock->lock;
			my $status = $work_data->{PIDs}{$pid}{status};
			$work_lock->unlock;
			( $FF_REAPED == $status ) or terminator( $pid, $signal );
		}
	}

	return 1;
}


# A warnings handler that lets perl warnings be printed via log
sub warnHandler {
	my ( $warn ) = @_;
	return log_warning( '%s', $warn );
}

sub wait_for_all_forks {
	$work_lock->lock;
	my @PIDs = ( sort keys %{ $work_data->{PIDs} } );
	$work_lock->unlock;

	foreach my $pid ( @PIDs ) {

		# Wait for PID
		while ( can_work && ( 0 == reap_pid( $pid ) ) ) {
			usleep( 50 );
		}

		remove_pid( $pid, 1 );

	} ## end of Clearing PIDs

	return 1;
}

sub wait_for_capture {
	my ( $kid ) = @_;

	$work_lock->lock;
	$work_data->{PIDs}{$kid}{status} = $FF_RUNNING;
	$work_lock->unlock;
	usleep( 0 ); ## Simulates a yield() without multi-threading (We have (a) fork(s) !)

	log_debug( 'Process %d forked out', $kid );

	# Now wait for the result
	while ( can_work && ( 0 == reap_pid( $kid ) ) ) {
		usleep( 50 );
	}

	return 1;
}

sub wait_for_startup {
	my ( $pid, $fork_data, $fork_lock ) = @_;

	# Wait until the work data is initialized
	$fork_lock->lock;
	while ( !defined( $fork_data->{PIDs}{$pid} ) ) {
		$fork_lock->unlock;
		usleep( 500 ); # poll each half millisecond
		$fork_lock->lock;
	}
	# Now we can tell the world that we are created
	$fork_data->{PIDs}{$pid}{status} = $FF_CREATED;
	$fork_lock->unlock;

	# Wait until we got started, poll each ms
	usleep( 1 ); ## A little "yield()" simulation
	$fork_lock->lock;
	while ( $FF_RUNNING != $fork_data->{PIDs}{$pid}{status} ) {
		$fork_lock->unlock;
		usleep( 500 ); # poll each half millisecond
		$fork_lock->lock;
	}
	$fork_lock->unlock;

	return 1;
}


# This is a watchdog function that displays progress and joins all threads nicely if needed
sub watch_my_forks {
	$work_lock->lock;
	my $result       = 1;
	my $forks_active = $work_data->{cnt};
	my %fork_timeout = ();
	my %fork_strikes = ();
	log_debug( 'Forks : %s', ( join ', ', keys %{ $work_data->{PIDs} } ));
	$work_lock->unlock;

	while ( $forks_active > 0 ) {
		my %prgData;
		$work_lock->lock;
		my @PIDs     = sort keys %{ $work_data->{PIDs} };
		my $fork_cnt = $work_data->{cnt};
		$work_lock->unlock;
		$forks_active = 0;

		can_work() or last;

		foreach my $pid ( @PIDs ) {
			pid_exists( $pid ) or next;
			$forks_active += handle_fork_progress( $pid, \%prgData, \%fork_timeout, \%fork_strikes );
			usleep( 0 );
		}
		( $forks_active > 0 ) or show_progress( $fork_cnt, $forks_active, \%prgData, 1 ) and next;
		show_progress( $fork_cnt, $forks_active, \%prgData, 0 );
		( $death_note > 0 ) and send_forks_the_kill();

		$work_lock->lock;
		@PIDs = sort keys %{ $work_data->{PIDs} };
		$work_lock->unlock;

		foreach my $pid ( @PIDs ) {
			can_work() or last;
			pid_exists( $pid ) or next;
			handle_fork_strikes( $pid, \%fork_timeout, \%fork_strikes );
		}

		usleep( 500_000 );
	}
	$result = wait_for_all_forks();
	return $result;
}

sub write_to_console {
	my ( $msg ) = @_;

	if ( $have_progress_msg > 0 ) {
		print "\n";
		$have_progress_msg = 0;
	}

	local $| = 1;
	return print "${msg}\n";
}

sub write_to_log {
	my ( $msg ) = @_;

	if ( open my $fLog, '>>', $logfile ) {
		print { $fLog } "${msg}\n";
		close $fLog or confess( "Closing logfile '$logfile' FAILED!" );
	}

	return 1;
}


__END__


=head1 NAME

Cleanup And Convert - cac


=head1 USAGE

cac [-h|OPTIONS] <-i INPUT [-i INPUT2...]> <-o OUTPUT>


=head1 ARGUMENTS

=over 8

=item B<-i | --input>

Path to the input file. Can appear more than once, resulting in the output file
to be the combination of the input files in their given order.

=item B<-o | --output>

The file to write. Must not equal any input file. Must have .mkv ending.

=back


=head1 OPTIONS

=over 8

=item B<-h | --help>

This help message

=item B<-t | --tempdir>

Path to the directory where the temporary files are written. Defaults to the
directory of the input file(s). Ensure to have 50x the space of the input!

=item B<-s | --splitaudio>

If set, split a second channel (if found) out into a separate .wav file. That
channel is normally live commentary, and will be discarded without this option.

=item B<-u | --upgrade>

Force a target of 60 FPS, even if the source is under 50 FPS.

=item B<-V | --version>

Print version and exit.

=back


=head1 DESCRIPTION

Cleanup And Convert: HurryKane's tool for overhauling gaming clips.
( See: @HurryKane76 yt channel )

The program uses ffmpeg to remove duplicate frames and to interpolate the video
to twice the target FPS in a first step, then do another search for duplicate
frames and interpolate down the the target FPS.

If the source has at least 50 FPS in average, the target is set to 60 FPS. For
sources with less than 50 FPS in average, the target is set to 30 FPS.
You can use the -u/--upgrade option to force the target to be 60 FPS, no matter
the source average.


=head1 REQUIRED ARGUMENTS

You have to provide at least one input file and one output file for the tool to
do anything useful. Every other possible argument is optional.


=head1 EXIT STATUS

The tools returns 0 on success and 1 on error.
If you kill the program with any signal that can be caught and handled, it will
do its best to end gracefully, and exit with exit code 42.


=head1 CONFIGURATION

Currently the only supported configuration are the command line arguments.


=head1 DEPENDENCIES

You will need a recent version of ffmpeg to make use of this tool.


=head1 DIAGNOSTICS

To find issues on odd program behavior, the -D/--debug command line argument can
be used. Please be warned, though, that the program becomes **very** chatty!

=head2 DEBUG MODE

=over 8

=item B<-D | --debug>

Displays extra information on all the steps of the way.
IMPORTANT: _ALL_ temporary files are kept! Use with caution!

=back


=head1 INCOMPATIBILITIES

I am pretty sure that this will not work on Windows. Sorry.


=head1 BUGS AND LIMITATIONS

Currently none known.

Please report bugs and/or errors at:
https://github.com/EdenWorX/ewxTools/issues


=head1 AUTHOR

Sven Eden <sven@eden-worx.com>


=head1 LICENSE AND COPYRIGHT

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


=cut

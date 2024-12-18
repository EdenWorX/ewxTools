#!/usr/bin/perl -w
use strict;
use warnings FATAL => 'all';

use PerlIO;
use POSIX qw( _exit floor :sys_wait_h );
use Symbol 'gensym';
use IPC::Open3;
use IPC::Shareable qw( LOCK_EX );
use IO::Select;
use Carp;
use Cwd qw( abs_path );
use Data::Dumper;
use File::Basename;
use File::ReadBackwards;
use Filesys::Df;
use Getopt::Long;
use List::MoreUtils qw( firstidx );
use Pod::Usage;
use Readonly;
use Time::HiRes qw( usleep );

Readonly my $main_pid => $$;  # Needed to know where we are
my $work_done = 0;            # Needed to know whether to log anything on END{}

# ===============
# === HISTORY ===
# ===============
# Version  Date        Maintainer     Changes
# 1.0.0    2024-05-23  sed, EdenWorX  First fully working version of the per variant. The Bash variant is dead now.
# 1.0.1    2024-05-29  sed, EdenWorX  Use IPC::Cmd::run_forked instead of using system()
# 1.0.2    2024-05-30  sed, EdenWorX  Rewrote the workers to be true forks instead of using iThreads.
# 1.0.3    2024-06-10  sed, EdenWorX  Greatly reduced complexity by untangling all the spaghetti code areas
# 1.0.4    2024-06-20  sed, EdenWorX  Review log system to produce easier to read log. Great for debugging!
#                                     If libplacebo freezes ffmpeg, which can happen although it is rare, kill the fork
#                                     and restart it using minterpolate instead. Better be slow than break.
# 1.0.5    2024-07-13  sed, EdenWorX  We no longer call for specific hardware initialization and let ffmpeg decide for
#                                       itself what hardware to use and how to use it (if any).
#                                     Also all forks now share knowledge about breaks and signals, so called processes
#                                       can be torn down, too. No more zombie processes if something goes wrong!
#                                     To make this work we switched to IPC::Open3 utilizing IO::Select.
# 1.0.6    2024-08-21  sed, EdenWorX  Split concatenating multiple sources from the segment creation, it is safer to do
#                                       this in two steps.
# 1.0.7    2024-09-02  sed, EdenWorX  Rework the code for terminating and restarting a frozen fork.
#   ( Version is the date from hereon )
# 24.09.09             sed, EdenWorX  New Version Scheme release
#                                     The classic <major>.<minor>.<patch> scheme does not say anything at all. The
#                                     reverse date scheme at least tells you the release date.
# 24.09.24             sed, EdenWorX  Allow users to specify maximum and target fps.
#
# Please keep this current:
Readonly our $VERSION => '24.09.24';

# =======================================================================================
# Workflow:
# Prepare: If multiple sources are set, concatenate them to one source MKV.
#          (Doing this and the split in one go often leads to splits that freeze ffmpeg.)
# Phase 1: Get Values via ffprobe and determine minimum seconds to split into 4 segments.
# Phase 2: Split the source into 4 segments, streamcopy, lengths from Phase 1.
# Phase 3: 1 Fork per Segment does mpdecimate(7)+libplacebo(120|60) into UTVideo.
# Phase 4: 1 Fork per Segment does mpdecimate(2)+libplacebo(60|30) into UTVideo.
# Phase 5: h264_nvenc produces output from all segments, highest quality
# Cleanup: segments and temporaries are to be deleted.
# Note   : We use h264 instead of x265, because the format is less expensive to decode,
#          and thus more performant in video editors, especially when seeking backwards.
# =======================================================================================

# ---------------------------------------------------------
# Shared Variables
# ---------------------------------------------------------
# signal handling
my $death_note = 0;

# Global return value, is set to 1 by log_error()
my $ret_global = 0;

# Fork status values
Readonly my $FF_CREATED  => 1;
Readonly my $FF_RUNNING  => 2;
Readonly my $FF_KILLED   => 3;
Readonly my $FF_FINISHED => 4;
Readonly my $FF_REAPED   => 5;

# ffmpeg progress values
Readonly my $PROGRESS_NONE     => 1;
Readonly my $PROGRESS_CONTINUE => 2;
Readonly my $PROGRESS_ENDED    => 3;

#@type IPC::Shareable
my $work_data = IPC::Shareable->new( key => 'WORK_DATA', create => 1 );

$work_data->{cnt}   = 0;
$work_data->{DEATH} = 0;              ## transports the death note after forking
$work_data->{MLEN}  = [ 0, 0, 0, 0 ];
$work_data->{PIDs}  = {};
$work_data->{ULEN}  = [ 0, 0, 0, 0 ];

Readonly my $EMPTY      => q{};
Readonly my $SPACE      => q{ };
Readonly my $EIGHTSPACE => q{        };  ## to blank the space for PID display

# ---------------------------------------------------------
# Logging facilities
# ---------------------------------------------------------
my $do_debug          = 0;
my $do_lock_debug     = 0;      ## Split out, as [un]lock debugging is _very_ noisy!
my $have_progress_msg = 0;
my $logfile           = $EMPTY;

Readonly my $LOG_DEBUG   => 1;
Readonly my $LOG_INFO    => 2;
Readonly my $LOG_STATUS  => 3;
Readonly my $LOG_WARNING => 4;
Readonly my $LOG_ERROR   => 5;

# ---------------------------------------------------------
# Signal Handlers
# ---------------------------------------------------------
Readonly my %SIGS_CAUGHT => ( 'INT' => 1, 'QUIT' => 1, 'TERM' => 1 );
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
Readonly my $B_decimate        => '[decim];[decim]';
Readonly my $B_in              => '[in]';
Readonly my $B_interp          => '[interp];[interp]';
Readonly my $B_middle          => '[middle];[middle]';
Readonly my $B_out             => '[out]';
Readonly my $defaultProbeSize  => 256 * 1_024 * 1_024;  # Max out probe size at 256 MB, all relevant stream info should be available from that size
Readonly my $defaultProbeDura  => 30 * 1_000 * 1_000;   # Max out analyze duration at 30 seconds. This should be long enough for everything
Readonly my $defaultProbeFPS   => 8 * 120;              # FPS probing is maxed at 8 seconds for 120 FPS recordings.
Readonly my $TIMEOUT_INTERVALS => 240;                  # Timeout for forks to start working (240 interval = 120 seconds = 2 minutes)

# ---------------------------------------------------------
# Global variables
# ---------------------------------------------------------
our $FF;
our $FP;
my @FF_ARGS_ACOPY_FIL  = qw( -map 0 -codec:a copy -vf );
my @FF_ARGS_CODEC_h264 = qw(
  -codec:v     h264_nvenc -preset:v p7     -tune:v     hq -profile:v   high444p -level:v   5.2
  -rc:v        vbr        -rgb_mode yuv444 -cq         4  -qmin        1        -qmax      16
  -temporal_aq 1          -b_adapt  0      -b_ref_mode 0  -zerolatency 1        -multipass 2
  -forced-idr  1 );
my @FF_ARGS_CODEC_UTV   = qw( -codec:v utvideo -pred median );
my @FF_ARGS_FILTER      = qw( -ignore_unknown -vf );
my @FF_ARGS_FORMAT      = qw( -colorspace bt709 -color_range pc -pix_fmt yuv444p -f matroska -write_crc32 0 );
my @FF_ARGS_INPUT_CAT   = qw( -f concat -safe 0 );
my @FF_ARGS_INPUT_INIT  = qw( -loglevel level+warning -nostats -colorspace bt709 -color_range pc );
my @FF_ARGS_START       = qw( -hide_banner -loglevel level+info -y );
my @FF_CONCAT_BEGIN     = qw( -loglevel level+warning -nostats -f concat -safe 0 -i );
my @FF_CONCAT_END       = qw( -map 0 -f matroska -write_crc32 0 -c copy );
my @FF_SEGMENT_BEGIN    = qw( -f segment -segment_time );
my @FF_SEGMENT_END      = qw( -map 0 -c copy );
my @FP_ARGS             = qw( -hide_banner -loglevel error -v quiet -show_format -of flat=s=_ -show_entries );
my $ff_interp_libp_none = "libplacebo='extra_opts=preset=high_quality:frame_mixer=none:fps=%d'";
my $ff_interp_libp_high = "libplacebo='extra_opts=preset=high_quality:frame_mixer=mitchell_clamp:fps=%d'";
my $ff_interp_mint_none = "minterpolate='fps=%d:mi_mode=dup:scd=none'";
my $ff_interp_mint_high = "minterpolate='fps=%d:mi_mode=mci:mc_mode=aobmc:me_mode=bidir:vsbmc=1'";

my $arg_max_fps      = 0;
my $arg_tgt_fps      = 0;
my %dir_stats        = ();                ## <dir> => { has_space => <what df says>, need_space => all inputs in there x 80, srcs => @src }
my $do_print_help    = 0;
my $do_print_version = 0;
my $do_split_audio   = 0;
my $force_upgrade    = 0;
my $max_fps          = 0;
my $maxProbeSize     = $defaultProbeSize;
my $maxProbeDura     = $defaultProbeDura;
my $maxProbeFPS      = $defaultProbeFPS;
my $target_fps       = 0;
my @path_source      = ();
my $path_target      = $EMPTY;
my $path_temp        = $EMPTY;
my $source_count     = 0;
my %source_groups    = ();
my %source_ids       = ();
my %source_info      = ();
my $video_stream     = 0;
my $audio_stream     = 0;
my $voice_stream     = -1;
my $audio_channels   = 0;
my $audio_layout     = 'guess';
my $voice_channels   = 0;
my $voice_layout     = 'guess';

# ---------------------------------------------------------
# BEGIN Handler
# ---------------------------------------------------------
BEGIN {
	# ffmpeg default values
	chomp( $FF = qx{which ffmpeg} );
	chomp( $FP = qx{which ffprobe} );
}  ## End BEGIN

# ---------------------------------------------------------
# Argument handling
# ---------------------------------------------------------
my $podmsg          = "\tcac ; HurryKane's [c]leanup [a]nd [c]onvert\n";
my %program_options = (
	'help|h'        => \$do_print_help,
	'debug|D'       => \$do_debug,
	'input|i=s'     => \@path_source,
	'lock-debug'    => \$do_lock_debug,
	'maxfps=i'      => \$arg_max_fps,
	'output|o=s'    => \$path_target,
	'splitaudio|s!' => \$do_split_audio,
	'tempdir|t:s'   => \$path_temp,
	'targetfps=i'   => \$arg_tgt_fps,
	'upgrade|u'     => \$force_upgrade,
	'version|V'     => \$do_print_version
);
GetOptions(%program_options) or pod2usage( { -message => $podmsg, -exitval => 2, -verbose => 0 } );
$do_print_help > 0 and pod2usage( { -exitval => 0, -verbose => 2, -noperldoc => 1 } );
$do_print_version > 0 and print "EWX cac V$VERSION\n" and exit 0;

# ---------------------------------------------------------
# Check Arguments
# ---------------------------------------------------------
check_arguments() > 0 and pod2usage( { -message => $podmsg, -exitval => 1, -verbose => 0 } );  ## The sub has already logged

( defined $FF ) and ( 0 < ( length $FF ) ) and -x $FF or log_error( $work_data, q{No ffmpeg available (FF: '%s')},  $FF // 'undef' ) and exit 3;
( defined $FP ) and ( 0 < ( length $FP ) ) and -x $FP or log_error( $work_data, q{No ffprobe available (FP: '%s')}, $FP // 'undef' ) and exit 3;

# ---------------------------------------------------------
# ================	  MAIN  PROGRAM	  ================
# ---------------------------------------------------------
$work_done = 1;  # From now on we consider this program as at work
log_status( $work_data, 'Processing %s start', $path_target );

# ---
# --- 1) we need information about each source file
# ---
analyze_all_inputs() and check_temp_dir() or exit 6;

# ---
# --- 2) All input files per temp directory have to be grouped. Each group is then segmented
# ---    Into four parts, so that four forks can do the interpolation in parallel per group.
# ---
build_source_groups() or declare_single_source() or exit 7;
segment_all_groups();
check_target_fps();

# ---
# --- 3) Now each groups segments can be decimated and interpolated up to max fps (round 1)
# ---
can_work() and log_status( $work_data, 'Interpolating segments up to %d FPS...', $max_fps );
foreach my $groupID ( sort { $a <=> $b } keys %source_groups ) {
	can_work() or last;
	my $inter_opts = {
		'dec_frac' => 0.5,
		'dec_max'  => 7,
		'do_alt'   => 0,
		'fps'      => $max_fps,
		'prg'      => $source_groups{$groupID}{prgu},
		'src'      => 'tmp',
		'tgt'      => 'iup',
	};
	interpolate_source_group( $groupID, $inter_opts ) or exit 9;
} ## end foreach my $groupID ( sort ...)

# ---
# --- 4) Then all groups segments have to be decimated and interpolated down to target fps (round 2)
# ---
can_work() and log_status( $work_data, 'Interpolating segments down to %d FPS...', $target_fps );
foreach my $groupID ( sort { $a <=> $b } keys %source_groups ) {
	can_work() or last;
	my $inter_opts = {
		'dec_frac' => 0.667,
		'dec_max'  => 3,
		'do_alt'   => 0,
		'fps'      => $target_fps,
		'prg'      => $source_groups{$groupID}{prgd},
		'src'      => 'iup',
		'tgt'      => 'idn'
	};
	interpolate_source_group( $groupID, $inter_opts ) or exit 10;
} ## end foreach my $groupID ( sort ...)

# ---
# --- 5) And finally we can put all the latest temp files together and create the target vid
# ---
if ( can_work() ) {
	log_status( $work_data, 'Creating %s ...', $path_target );
	my $inter_opts = {
		'src'      => 'idn',
		'tgt'      => 'out',
		'dec_max'  => 5,
		'dec_frac' => 0.8,
		'fps'      => $target_fps,
		'do_alt'   => 0
	};
	assemble_output($inter_opts);
} ## end if ( can_work() )

# ---------------------------------------------------------
# END Handler
# ---------------------------------------------------------
END {
	# The END is only of any importance if this is the main fork.
	if ( $$ == $main_pid ) {
		wait_for_all_forks();

		# Let's clean up and remove all temporary files, if this is "release" mode,
		# or at least list all "orphaned" files if this is debug mode
		( $work_done > 0 ) and cleanup_source_groups();

		log_status( $work_data, 'Program %s', ( 0 == $ret_global ) ? 'finished' : 'FAILED!' );

		IPC::Shareable->clean_up;

	} ## end if ( $$ == $main_pid )
} ## end END

exit $ret_global;

# ---------------------------------------------------------
# ================ FUNCTION IMPLEMENTATION ================
# ---------------------------------------------------------

##
# @brief Add a process ID to work_data.
#
# @param $pid A valid process ID. This function will throw a confess() if an invalid PID is added.
# @param $gid The source group id the file processed by this PID belongs to.
#
# @return Always returns 1.
#
# @details This subroutine adds a process ID to the `work_data` hash.
#          It initializes the process ID with some default data including args, exit code,
#          error message, program file, result, status, source and target.
#          It also increments the `work_data->{cnt}` by 1.
#          If the process ID is already present in work_data, the subroutine will throw a confess().
#
# @warning This subroutine should be used cautiously as it does modify the global `work_data` structure.
#
sub add_pid {
	my ( $pid, $gid ) = @_;
	( defined $pid ) and ( $pid =~ m/^\d+$/ms )
	  or log_error( $work_data, "add_pid(): BUG! '%s' is not a valid pid!", $pid // 'undef' )
	  and confess('FATAL BUG!');
	( defined $gid ) and ( $gid =~ m/^\d+$/ms )
	  or log_error( $work_data, "add_pid(): BUG! '%s' is not a valid gid!", $gid // 'undef' )
	  and confess('FATAL BUG!');
	defined( $work_data->{PIDs}{$pid} ) and confess("add_pid($pid) called but work_data already defined!");
	lock_data($work_data);
	log_debug( $work_data, 'Adding PID %d for GID %d ...', $pid, $gid );
	$work_data->{PIDs}{$pid} = {
		args      => [],     ## Shall be added by the caller as a reference
		exit_code => 0,
		error_msg => $EMPTY,
		gid       => $gid,
		id        => 0,
		prgfile   => $EMPTY,
		result    => $EMPTY,
		status    => 0,
		source    => $EMPTY,
		target    => $EMPTY
	};
	unlock_data($work_data);
	return pid_count_inc();
} ## end sub add_pid

##
#  @brief This subroutine analyzes all input data sources.
#
#  @details This routine starts by checking if the system can work using can_work() subroutine.
#           If it can work, it will then loop over each source in @path_source array. For each source,
#           it assesses its size and prepares source information including avg_frame_rate, dir, duration,
#           etc.
#           It also links each source id with its related source in the $source_ids hash.
#           Each source is then analyzed using the analyze_input() subroutine.
#
#  @return Returns a flag (1) to indicate successful execution (standard Perl true). If the system can't
#          work, the routine will terminate early, returning 1. If a source can't be analyzed, the routine
#          will exit with status code 6.
#
#  @warning This subroutine exits the program with status code 6 in case of error during individual source
#           analysis.
#
#  @note This subroutine uses a variety of Perl built-ins and external functions for its operation.
#
#  @param None.
#
sub analyze_all_inputs {
	can_work() or return 1;
	my $pathID = 0;  ## Counter for the source id hash

	foreach my $src (@path_source) {
		can_work() or last;

		my $inSize = -s $src;

		## no critic (ProhibitParensWithBuiltins)
		$source_info{$src} = {
			avg_frame_rate => 0,
			bit_rate       => 0,
			dir            => dirname($src),
			duration       => 0,
			id             => ++$pathID,
			probeSize      => $inSize > $maxProbeSize ? $maxProbeSize : $inSize,
			probeStrings   => sprintf( '-probesize %d', $inSize > $maxProbeSize ? $maxProbeSize : $inSize )
		};
		$source_ids{$pathID} = $src;

		analyze_input($src) or exit 6;
	} ## end foreach my $src (@path_source)

	return 1;
} ## end sub analyze_all_inputs

sub analyze_input {
	my ($src) = @_;

	my $formats = $source_info{$src};  ## Shortcut

	# Get basic duration
	my $stream_fields = 'avg_frame_rate,duration';
	my $frstream_no   = get_info_from_ffprobe( $src, $stream_fields );
	( $frstream_no >= 0 ) or return 0;        ## Something went wrong
	my $streams  = $formats->{streams};       ## shortcut, too
	my $frstream = $streams->[$frstream_no];  ## shortcut, three

	( defined $formats->{duration} ) and ( $formats->{duration} > 0 )
	  or log_error( $work_data, "Unable to determine duration of '%s'", $src )
	  and return 0;
	( defined $frstream->{avg_frame_rate} ) and ( $frstream->{avg_frame_rate} > 0 )
	  or log_error( $work_data, "Unable to determine average frame rate of '%s'", $src )
	  and return 0;

	log_debug( $work_data, 'Duration   : %d', $formats->{duration} );
	log_debug( $work_data, 'Average FPS: %d', $frstream->{avg_frame_rate} );

	$formats->{probedDuration} = $formats->{duration} * 1000 * 1000;  ## Probe Duration is set up in microseconds.
	$formats->{probeFPS}       = $frstream->{avg_frame_rate} * 8;
	( $formats->{probedDuration} > $maxProbeDura ) and $formats->{probedDuration} = $maxProbeDura;
	( $formats->{probeFPS} > $maxProbeFPS )        and $formats->{probeFPS}       = $maxProbeFPS;

	$formats->{sourceFPS}    = $frstream->{avg_frame_rate};
	$formats->{probeStrings} = sprintf '-probesize %d -analyzeduration %d -fpsprobesize %d', $formats->{probeSize}, $formats->{probedDuration}, $formats->{probeFPS};

	# Now that we have good (and sane) values for probing sizes and durations, lets query ffprobe again to get the final value we need.
	can_work() or return 0;
	$stream_fields = 'avg_frame_rate,channels,codec_name,codec_type,nb_streams,pix_fmt,r_frame_rate,stream_type,duration';
	$frstream      = get_info_from_ffprobe( $src, $stream_fields );
	( $frstream_no >= 0 ) or return 0;                                ## Something went wrong this time
	$frstream = $streams->[$frstream_no];                             ## shortcut, four...

	# Maybe we have to fix the duration values we currently have
	$formats->{duration} =~ m/(\d+[.]\d+)/ms
	  and $formats->{duration} = floor( 1. + ( 1. * $1 ) );

	# Now we can go through the read stream information and determine video and audio stream details
	analyze_stream_info( $src, $streams ) or log_error( $work_data, "Analyzing '%s' FAILED!", $src ) and return 0;

	# If the second analysis somehow came up with a different average framerate, we have to adapt:
	if (   ( $formats->{duration} > 0 )
		&& ( $frstream->{avg_frame_rate} > 0 )
		&& ( $formats->{sourceFPS} != $formats->{avg_frame_rate} ) )
	{
		$formats->{probedDuration} = $formats->{duration} * 1000 * 1000;  ## Probe Duration is set up in microseconds.
		$formats->{probeFPS}       = $frstream->{avg_frame_rate} * 8;

		log_debug( $work_data, '(fixed) Duration   : %d', $formats->{duration} );
		log_debug( $work_data, '(fixed) Average FPS: %d', $frstream->{avg_frame_rate} );

		( $formats->{probedDuration} > $maxProbeDura ) and $formats->{probedDuration} = $maxProbeDura;
		( $formats->{probeFPS} > $maxProbeFPS )        and $formats->{probeFPS}       = $maxProbeFPS;

		$formats->{sourceFPS} = $frstream->{avg_frame_rate};
	} ## end if ( ( $formats->{duration...}))

	return 1;
} ## end sub analyze_input

sub analyze_stream_info {
	my ( $src, $streams ) = @_;

	can_work() or return 0;

	my $have_video = 0;
	my $have_audio = 0;
	my $have_voice = 0;

	for ( 0 .. ( $source_info{$src}{nb_streams} - 1 ) ) {
		my $i = $_;  ## save the magic bullet
		if ( ( defined $streams->[$i]{codec_type} ) && ( $streams->[$i]{codec_type} eq 'video' ) ) {
			$have_video   = 1;
			$video_stream = $i;
		}
		if ( ( defined $streams->[$i]{codec_type} ) && ( $streams->[$i]{codec_type} eq 'audio' ) ) {
			if ( 0 == $have_audio ) {
				$have_audio   = 1;
				$audio_stream = $i;
				if ( $streams->[$i]{channels} > $audio_channels ) {
					$audio_channels = $streams->[$i]{channels};
					$audio_layout   = channels_to_layout($audio_channels);
				}
			} elsif ( 0 == $have_voice ) {
				$have_voice   = 1;
				$voice_stream = $i;
				if ( $streams->[$i]{channels} > $voice_channels ) {
					$voice_channels = $streams->[$i]{channels};
					$voice_layout   = channels_to_layout($voice_channels);
				}
			} else {
				log_error( $work_data, "Found third audio channel in '%s' - no idea what to do with it!", $src );
				return 0;
			}
		} ## end if ( ( defined $streams...))
	} ## end for ( 0 .. ( $source_info...))
	( 0 == $have_video ) and log_error( $work_data, "Source file '%s' has no video stream!", $src ) and return 0;

	return 1;
} ## end sub analyze_stream_info

sub assemble_output {
	my ($inter_opts) = @_;

	can_work() or return 1;
	my $tmpdir  = ( 0 == ( length $path_temp ) ) ? dirname($path_target) : $path_temp;
	my $lstfile = sprintf '%s/temp_%d_src.lst', $tmpdir, $main_pid;
	my $prgfile = sprintf '%s/temp_%d_prg.log', $tmpdir, $main_pid;
	my $mapfile = $path_target;
	$mapfile =~ s/[.]mkv$/.wav/ms;

	if ( open my $fOut, '>', $lstfile ) {
		foreach my $groupID ( sort { $a <=> $b } keys %source_groups ) {
			for my $i ( 0 .. 3 ) {
				my $outname = abs_path( sprintf $source_groups{$groupID}{idn}, $i );
				printf {$fOut} "file '%s'\n", $outname;
				log_debug( $work_data, 'Adding "%s" to output source list', $outname );
			}
		} ## end foreach my $groupID ( sort ...)
		close $fOut or croak("Closing listfile '$lstfile' FAILED!");
	} else {
		log_error( $work_data, "Unable to write into '%s': %s", $lstfile, $! );
		exit 11;
	}

	# Having a list file we can go and create our output:
	if ( can_work() ) {
		create_target_file( $lstfile, $prgfile, $mapfile, $inter_opts ) or exit 12;
	}

	# When everything is good, we no longer need the list file, progress file and the temp files
	if ( 0 == $do_debug ) {
		-f $lstfile and unlink $lstfile;
		-f $prgfile and unlink $prgfile;
		foreach my $groupID ( sort { $a <=> $b } keys %source_groups ) {
			for my $i ( 0 .. 3 ) {
				my $tmpfile = sprintf $source_groups{$groupID}{idn}, $i;
				log_debug( $work_data, 'Removing %s...', $tmpfile );
				-f $tmpfile and unlink $tmpfile;
			}
		} ## end foreach my $groupID ( sort ...)
	} ## end if ( 0 == $do_debug )

	return 1;
} ## end sub assemble_output

sub build_source_groups {
	can_work()            or return 1;
	( $source_count > 1 ) or return 0;
	my $group_id    = 0;
	my $last_dir    = ( 0 == ( length $path_temp ) ) ? 'n/a' : $path_temp;
	my $last_ch_cnt = 0;
	my %last_codec  = ();
	my $tmp_count   = 0;

	foreach my $fileID ( sort { $a <=> $b } keys %source_ids ) {
		can_work() or last;
		my $src  = $source_ids{$fileID};
		my $data = $source_info{$src};   ## shortcut

		# The next group is needed, if channel count, any codec or the directory changes.
		my $dir_changed   = ( 0 == ( length $path_temp ) ) && ( $data->{dir} ne $last_dir );
		my $ch_changed    = ( $data->{nb_streams} != $last_ch_cnt );
		my $codec_changed = 0;

		# Codecs must be looked at in a loop, as we do not know how many there are.
		for ( 0 .. ( $data->{nb_streams} - 1 ) ) {
			( ( !( defined $last_codec{$_} ) ) or ( $last_codec{$_} ne $data->{streams}[$_]{codec_name} ) )
			  and $codec_changed = 1;
			$last_codec{$_} = $data->{streams}[$_]{codec_name};
		}
		$last_dir    = ( 0 == ( length $path_temp ) ) ? $data->{dir} : $path_temp;
		$last_ch_cnt = $data->{nb_streams};

		# Let's start a new group if anything changed
		if ( ( $dir_changed + $ch_changed + $codec_changed ) > 0 ) {
			## no critic (ProhibitParensWithBuiltins)
			$source_groups{ ++$group_id } = {
				cat  => sprintf( '%s/temp_%d_segments_%d_src.mkv', $last_dir, $main_pid, ++$tmp_count ),
				cnt  => 0,
				dir  => abs_path($last_dir),
				dur  => 0,
				fps  => 0,
				idn  => sprintf( '%s/temp_%d_inter_dn_%d_%%d.mkv', $last_dir, $main_pid, ++$tmp_count ),
				ids  => [],
				iup  => sprintf( '%s/temp_%d_inter_up_%d_%%d.mkv',    $last_dir, $main_pid, ++$tmp_count ),
				lst  => sprintf( '%s/temp_%d_segments_%d_src.lst',    $last_dir, $main_pid, ++$tmp_count ),
				prgu => sprintf( '%s/temp_%d_progress_up_%d_%%d.prg', $last_dir, $main_pid, ++$tmp_count ),
				prgd => sprintf( '%s/temp_%d_progress_dn_%d_%%d.prg', $last_dir, $main_pid, ++$tmp_count ),
				srcs => [],
				tmp  => sprintf( '%s/temp_%d_segments_%d_%%d.mkv', $last_dir, $main_pid, ++$tmp_count )
			};
		} ## end if ( ( $dir_changed + ...))

		# Now add the file
		$source_groups{$group_id}{dur} += $data->{duration};
		$data->{sourceFPS} > $source_groups{$group_id}{fps}
		  and $source_groups{$group_id}{fps} = $data->{sourceFPS};
		$source_groups{$group_id}{cnt} += 1;
		push @{ $source_groups{$group_id}{ids} },  $fileID;
		push @{ $source_groups{$group_id}{srcs} }, abs_path($src);
	}  ## End of grouping input files

	return 1;
} ## end sub build_source_groups

sub can_work {
	if ( 0 != $ret_global ) {

		# A non-zero ret_global means: We have to go!
		( $death_note > 0 ) or $death_note = 1;
	}
	return ( ( 0 == $death_note ) && ( 0 == $ret_global ) );
} ## end sub can_work

# ----------------------------------------------------------------
# Simple Wrapper around IPC::Cmd to capture simple command outputs
# ----------------------------------------------------------------
sub capture_cmd {
	my (@cmd) = @_;
	my $kid = fork;

	( defined $kid ) or croak("Cannot fork()! $!\n");

	# Handle being the fork first
	# =======================================
	if ( 0 == $kid ) {
		start_capture( \@cmd );
		POSIX::_exit(0);  ## Regular exit() would call main::END block
	}

	# === Do the bookkeeping before we wait
	# =======================================
	add_pid( $kid, 0 );

	# Wait on the fork to finish
	# =======================================
	wait_for_capture($kid);

	# Handle result:
	lock_data($work_data);
	if ( defined( $work_data->{PIDs}{$kid}{exit_code} ) && ( 0 != $work_data->{PIDs}{$kid}{exit_code} ) ) {
		log_error(
			$work_data,
			"Command '%s' FAILED [%d] :\nSTDOUT: %s\nSTDERR: %s",
			join( $SPACE, @cmd ),
			$work_data->{PIDs}{$kid}{exit_code},
			$work_data->{PIDs}{$kid}{result},
			$work_data->{PIDs}{$kid}{error_msg}
		);
		unlock_data($work_data);
		croak('capture_cmd() crashed');
	} ## end if ( defined( $work_data...))

	my $result = $work_data->{PIDs}{$kid}{result};
	unlock_data($work_data);

	remove_pid( $kid, 1 );

	return $result;
} ## end sub capture_cmd

sub channels_to_layout {
	my ($channels) = @_;
	( 1 == $channels ) and return 'mono';
	( 2 == $channels ) and return 'stereo';
	( 3 == $channels ) and return '2.1';
	( 4 == $channels ) and return 'quad';
	( 5 == $channels ) and return '4.1';
	( 6 == $channels ) and return '5.1';
	( 7 == $channels ) and return '6.1';
	( 8 == $channels ) and return '7.1';
	return 'guess';
} ## end sub channels_to_layout

sub check_arguments {
	my $errCount    = 0;
	my $have_source = 0;
	my $have_target = 0;

	check_source_and_target( \$errCount, \$have_source, \$have_target );
	$have_source and check_input_files( \$errCount );
	$have_target and check_output_existence( \$errCount );

	return $errCount;
} ## end sub check_arguments

sub check_input_files {
	my ($errCount) = @_;
	foreach my $src (@path_source) {
		validate_input_file($src) or ${$errCount}++;
	}
	return 1;
} ## end sub check_input_files

sub check_logger {
	my ($logger) = @_;
	if ( defined $logger ) {
		$logger =~ m/^log_(info|warning|error|status|debug)$/xms
		  or confess("logMsg() called from wrong sub $logger");
	}
	return 1;
} ## end sub check_logger

sub check_output_existence {
	my ($errCount) = @_;

	-f $path_target and log_error( $work_data, "Output file '%s' already exists!", $path_target ) and ++${$errCount};
	$path_target =~ m/[.]mkv$/ms or log_error( $work_data, "Output file '%s' does not have mkv ending!", $path_target ) and ++${$errCount};
	foreach my $src (@path_source) {
		$src eq $path_target and log_error( $work_data, "Input file '%s' equals output file!", $src ) and ++${$errCount};
	}

	return 1;
} ## end sub check_output_existence

sub check_pid {
	my ($pid) = @_;

	( defined $pid ) and ( $pid =~ m/^\d+$/ms )
	  or log_error( $work_data, "BUG! '%s' is not a valid pid!", $pid // 'undef' )
	  and confess('FATAL BUG!');

	( defined $work_data->{PIDs}{$pid} ) or return 0;

	return 1;
} ## end sub check_pid

# If we are in the middle of a frozen PID restart, the above loop might have tried to question
# a PID that was already substituted. In that case the old PID would have been gone, but it
# is no longer listed as being about to start. In that case we might think the PID crashed,
# while we are just late to the party.
# So let's check all PID data again, before we force the whole thing down.
sub check_pids_crashed {
	my @PIDs         = @_;
	my $forks_found  = 0;
	my $pids_crashed = 0;

	log_debug( $work_data, 'Found %d PIDs that might have crashed. Checking PIDs...', $pids_crashed );

	lock_data($work_data);
	my $forks_active = $work_data->{cnt};
	unlock_data($work_data);

	foreach my $pid (@PIDs) {
		my $is_active = is_pid_active($pid);
		( $is_active > 0 ) and ++$forks_found  and log_debug( $work_data, 'PID %d found active' );
		( $is_active < 0 ) and ++$pids_crashed and log_debug( $work_data, 'PID %d found CRASHED!' );
	}

	log_debug( $work_data, '%d/%d PID%s found working, %d PID%s crashed.',
		$forks_found, $forks_active, plural_s($forks_found), $pids_crashed, plural_s($pids_crashed) );

	return $pids_crashed;
} ## end sub check_pids_crashed

sub check_source_and_target {
	my ( $errCount, $have_source, $have_target ) = @_;

	${$have_source} = scalar @path_source;
	${$have_target} = length $path_target;

	${$have_target} and set_log_file() and ${$have_source} and return 1;

	${$have_source} or log_error( $work_data, 'No Input given!' )  and ++${$errCount};
	${$have_target} or log_error( $work_data, 'No Output given!' ) and ++${$errCount};

	return 0;
} ## end sub check_source_and_target

sub check_multi_temp_dir {
	my $errCount = 0;

	foreach my $src (@path_source) {
		if ( -f $src ) {
			my $dir = dirname($src);
			( length $dir ) > 0 or $dir = q{.};
			if ( !( defined $dir_stats{$dir} ) ) {
				$dir_stats{$dir} = { has_space => 0, need_space => 0, srcs => [] };
			}
			push @{ $dir_stats{$dir}{srcs} }, $src;
			my $ref = df($dir);
			if ( ( defined $ref ) ) {
				my $size_factor = get_size_factor($src);
				my $size        = ( -s $src ) / 1024 / 1024;  ## Size of the file in MiB blocks

				$dir_stats{$dir}{has_space}  += $ref->{bavail} / 1024;  ## df returns 1K blocks, but we calculate in M.
				$dir_stats{$dir}{need_space} += $size * $size_factor;   ## also in M now, $size_factor times the source size.
			} else {

				# =) df() failed? WTF?
				log_error( $work_data, "df'ing directory '%s' FAILED!", $dir ) and ++$errCount;
			}
		}  # No else, that error has already been recorded under Test 1
	} ## end foreach my $src (@path_source)
	## Now check the stats...
	foreach my $dir ( sort keys %dir_stats ) {
		$dir_stats{$dir}{need_space} > $dir_stats{$dir}{has_space} and log_error(
			$work_data, "Not enough space! '%s' has only %s / %s M free!",
			$dir,
			cleanint( $dir_stats{$dir}{has_space} ),
			cleanint( $dir_stats{$dir}{need_space} )
		  )
		  and ++$errCount
		  or log_info( $work_data, "Directory '%s' needs approximately %s MiB space for temporary files.", $dir, cleanint( $dir_stats{$dir}{need_space} ) );
	} ## end foreach my $dir ( sort keys...)

	return $errCount > 0 ? 0 : 1;
} ## end sub check_multi_temp_dir

sub check_single_temp_dir {
	my $errCount = 0;

	if ( -d $path_temp ) {

		# =) Temp Dir exists
		my $ref = df($path_temp);
		$dir_stats{$path_temp} = { has_space => 0, need_space => 0, srcs => [] };
		foreach my $src (@path_source) {
			push @{ $dir_stats{$path_temp}{srcs} }, $src;
		}

		if ( defined $ref ) {

			# We have to accumulate all sizes and estimated temp sizes
			$dir_stats{$path_temp}{need_space} = 0;
			foreach my $src (@path_source) {
				if ( -f $src ) {
					my $size_factor = get_size_factor($src);
					my $size        = ( -s $src ) / 1024 / 1024;  ## Size of the file in MiB blocks

					$dir_stats{$path_temp}{need_space} += $size * $size_factor;  ## also in M now, $size_factor times the source size.
				}  # No else, that error has already been recorded under Test 1
			} ## end foreach my $src (@path_source)

			# Note: See check_multi_temp_dir() about the sizes.
			$dir_stats{$path_temp}{has_space} = $ref->{bavail} / 1024;
			$dir_stats{$path_temp}{need_space} > $dir_stats{$path_temp}{has_space} and log_error(
				$work_data, "Not enough space! '%s' has only %s / %s M free!",
				$path_temp,
				cleanint( $dir_stats{$path_temp}{has_space} ),
				cleanint( $dir_stats{$path_temp}{need_space} )
			  )
			  and ++$errCount
			  or log_info( $work_data, "Directory '%s' needs approximately %s MiB space for temporary files.", $path_temp,
				cleanint( $dir_stats{$path_temp}{need_space} ) );
		} else {

			# =) df() failed? WTH?
			log_error( $work_data, "df'ing directory '%s' FAILED!", $path_temp ) and ++$errCount;
		}
	} else {

		# =) Temp Dir does NOT exist
		log_error( $work_data, "Temp directory '%s' does not exist!", $path_temp ) and ++$errCount;
	}

	return $errCount > 0 ? 0 : 1;
} ## end sub check_single_temp_dir

# Make sure we have a sane target FPS. max_fps is reused as upper fps
sub check_target_fps {
	can_work() or return 1;

	# First set defaults:
	$target_fps = ( ( $max_fps < 50 ) && ( 0 == $force_upgrade ) ) ? 30 : 60;
	if ( $max_fps < ( 2 * $target_fps ) ) {

		# Only use 2x the target FPS, as maximum, if it does not mean a downscaling.
		# If we downscale here, like from 144 to 120 FPS, we will already produce
		# mixed frames on high movement scenes, and blended frames due to 144 FPS
		# timing being _very_ different from 120 FPS timing.
		$max_fps = 2 * $target_fps;
	} ## end if ( $max_fps < ( 2 * ...))

	# If the maximum FPS set by the user is larger than the currently set maximum FPS,
	# we'll use the users choice.
	( $arg_max_fps > $max_fps ) and $max_fps = $arg_max_fps;

	# If the user has set a target FPS, we use that one unless it exceeds maximum FPS
	( $arg_tgt_fps > 0 ) and $target_fps = $arg_tgt_fps;

	# Target FPS must not exceed maximum FPS
	( $target_fps > $max_fps ) and $target_fps = $max_fps;

	log_info( $work_data, 'Decimate and interpolate up to %d FPS', $max_fps );
	log_info( $work_data, 'Then interpolate to the target %d FPS', $target_fps );
	return 1;
} ## end sub check_target_fps

sub check_temp_dir {
	return ( ( length $path_temp ) > 0 ) ? check_single_temp_dir() : check_multi_temp_dir();
}

sub cleanint {
	my ($float) = @_;
	my $int = floor($float);
	return commify($int);
}

sub cleanup_pid {
	my ($pid) = @_;
	my $result = 1;

	lock_data($work_data);

	# If we shall clean up source and maybe target files, do so now
	log_debug( $work_data, 'args      => %d', scalar @{ $work_data->{PIDs}{$pid}{args} // [] } );
	log_debug( $work_data, 'exit_code => %d', $work_data->{PIDs}{$pid}{exit_code} // 0 );
	log_debug( $work_data, 'id        => %d', $work_data->{PIDs}{$pid}{id}        // -1 );
	log_debug( $work_data, 'prgfile   => %s', $work_data->{PIDs}{$pid}{prgfile}   // 'undef' );
	log_debug( $work_data, 'source    => %s', $work_data->{PIDs}{$pid}{source}    // 'undef' );
	log_debug( $work_data, 'target    => %s', $work_data->{PIDs}{$pid}{target}    // 'undef' );
	log_debug( $work_data, 'STDOUT    => %s', $work_data->{PIDs}{$pid}{result}    // 'undef' );
	log_debug( $work_data, 'STDERR    => %s', $work_data->{PIDs}{$pid}{error_msg} // 'undef' );

	my $have_error = handle_fork_message( $work_data->{PIDs}{$pid}{error_msg} // $EMPTY );

	if ( ( defined( $work_data->{PIDs}{$pid}{exit_code} ) && ( $work_data->{PIDs}{$pid}{exit_code} != 0 ) ) || $have_error ) {
		$result = pid_shall_restart( $work_data, $pid );  ## We _did_ fail unless a restart was triggered.
		log_error(
			$work_data, "Worker PID %d %s [%d]!\n%s",
			$pid,
			$result ? 'killed for restart' : 'FAILED',
			$work_data->{PIDs}{$pid}{exit_code},
			$work_data->{PIDs}{$pid}{result}
		);

		# We do not need the target file any more, the thread failed! (if an fmt is set)
		if ( ( 0 == $do_debug ) && ( length( $work_data->{PIDs}{$pid}{target} ) > 0 ) ) {
			my $f = sprintf $work_data->{PIDs}{$pid}{target}, $work_data->{PIDs}{$pid}{id};
			log_debug( $work_data, "Removing target file '%s' ...", $f );
			-f $f and unlink $f;
		}
	} ## end if ( ( defined( $work_data...)))

	# We do not need the source file any more (if an fmt is set)
	if ( ( 0 == $do_debug ) && defined( $work_data->{PIDs}{$pid}{source} ) && ( length( $work_data->{PIDs}{$pid}{source} ) > 0 ) ) {
		my $f = sprintf $work_data->{PIDs}{$pid}{source}, $work_data->{PIDs}{$pid}{id};
		log_debug( $work_data, "Removing source file '%s' ...", $f );
		-f $f and unlink $f;
	}

	return $result;
} ## end sub cleanup_pid

sub cleanup_source_groups {
	foreach my $gid ( sort { $a <=> $b } keys %source_groups ) {
		if ( ( defined $source_groups{$gid}{lst} ) && ( -f $source_groups{$gid}{lst} ) ) {
			( $do_debug > 0 ) and log_debug( $work_data, 'See: %s', $source_groups{$gid}{lst} ) or unlink $source_groups{$gid}{lst};
		}
		for my $area (qw( tmp idn iup prg )) {
			( defined $source_groups{$gid}{$area} ) or next;
			for my $i ( 0 .. 3 ) {
				my $f = sprintf $source_groups{$gid}{$area}, $i;
				if ( -f $f ) {
					( $do_debug > 0 ) and log_debug( $work_data, 'See: %s', $f ) or unlink $f;
				}
			} ## end for my $i ( 0 .. 3 )
		} ## end for my $area (qw( tmp idn iup prg ))
	} ## end foreach my $gid ( sort { $a...})

	( $ret_global > 0 ) and log_error( $work_data, 'Processing %s FAILED!', $path_target ) or log_info( $work_data, 'Processing %s finished', $path_target );

	( ( 0 < ( length $logfile ) ) && ( -f $logfile ) )
	  and ( ( $ret_global > 0 ) || ( 1 == $do_debug ) )
	  and printf "\nSee %s for details\n", $logfile;

	return 1;
} ## end sub cleanup_source_groups

sub close_standard_io {
	my $devnull = '/dev/null';
	## no critic (RequireCheckedClose, RequireCheckedSyscalls)
	close STDIN  and open STDIN,  '<', $devnull;
	close STDOUT and open STDOUT, '>', $devnull;
	close STDERR and open STDERR, '>', $devnull;
	return 1;
} ## end sub close_standard_io

sub create_target_file {
	my ( $lstfile, $prgfile, $mapfile, $inter_opts ) = @_;
	can_work() or return 1;

	# The filters enforce the target FPS as cfr stream, and get rid of duplicates caused by concatting the parts.
	my $filter_string = make_filter_string( -1, $inter_opts );

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

	# Building the worker fork is quite trivial
	can_work() or return 1;
	my @fps_opts = ( '-fps_mode', 'cfr', '-r', $target_fps );
	my @ffargs   = (
		$FF,                 @FF_ARGS_START,     '-progress',         $prgfile, ( ( 'guess' ne $audio_layout ) ? qw( -guess_layout_max 0 ) : () ),
		@FF_ARGS_INPUT_INIT, @FF_ARGS_INPUT_CAT, '-i',                $lstfile,     @mapAudio, @metaAudio, @FF_ARGS_FILTER, $filter_string,
		@fps_opts,           @FF_ARGS_FORMAT,    @FF_ARGS_CODEC_h264, $path_target, @mapVoice
	);

	log_info( $work_data, "Starting Worker 1 for:\n%s", ( join $SPACE, @ffargs ) );
	my $pid = start_work( 1, 0, @ffargs );
	( defined $pid ) and ( $pid > 0 ) or confess('BUG! start_work() returned invalid PID!');
	lock_data($work_data);
	@{ $work_data->{PIDs}{$pid}{args} } = @ffargs;
	$work_data->{PIDs}{$pid}{prgfile} = $prgfile;
	unlock_data($work_data);

	# Watch and join
	return watch_my_forks();
} ## end sub create_target_file

sub commify {
	my ($text) = @_;
	$text = reverse $text;
	$text =~ s/(\d\d\d)(?=\d)(?!\d*[.])/$1,/gxms;
	return scalar reverse $text;
} ## end sub commify

sub concat_source_group {
	my ( $gid, $prgfile_fmt ) = @_;
	my $result = 1;
	can_work() or return 1;

	# To concatenate we use the concat demuxer. It needs an input file which lists the sources:
	if ( open my $fOut, '>', $source_groups{$gid}{lst} ) {
		foreach my $fid ( sort { $a <=> $b } @{ $source_groups{$gid}{ids} } ) {
			printf {$fOut} "file '%s'\n", abs_path( $source_ids{$fid} );
		}
		close $fOut or confess("Closing listfile '$source_groups{$gid}{lst}' FAILED!");
	} else {
		log_error( $work_data, q{Cannot write list file '%s': %s}, $source_groups{$gid}{lst}, $! );
		return 0;
	}

	# Let's build the command line arguments:
	my $prgfile = sprintf $prgfile_fmt, 1;
	my @ffargs  = (
		$FF,              @FF_ARGS_START, '-progress', $prgfile, ( ( 'guess' ne $audio_layout ) ? qw( -guess_layout_max 0 ) : () ),
		@FF_CONCAT_BEGIN, $source_groups{$gid}{lst},
		@FF_CONCAT_END,   $source_groups{$gid}{cat}
	);

	log_info( $work_data, "Starting Worker %d for:\n%s", 1, ( join $SPACE, @ffargs ) );
	my $pid = start_work( 1, $gid, @ffargs );
	( defined $pid ) and ( $pid > 0 ) or croak('BUG! start_work() returned invalid PID!');
	lock_data($work_data);
	@{ $work_data->{PIDs}{$pid}{args} } = @ffargs;
	$work_data->{PIDs}{$pid}{prgfile} = $prgfile;
	unlock_data($work_data);

	# Watch and join
	$result = watch_my_forks();

	# The list file is no longer needed.
	-f $source_groups{$gid}{lst} and ( 0 == $do_debug ) and unlink $source_groups{$gid}{lst};

	# As is the progress file
	-f $prgfile and ( 0 == $do_debug ) and unlink $prgfile;

	return $result;
} ## end sub concat_source_group

sub declare_single_source {
	can_work()             or return 1;
	( 1 == $source_count ) or return 0;
	my $src      = $path_source[0];
	my $data     = $source_info{$src};                                         ## shortcut
	my $fileID   = $data->{id};
	my $last_dir = ( 0 == ( length $path_temp ) ) ? $data->{dir} : $path_temp;

	## no critic (ProhibitParensWithBuiltins)
	$source_groups{0} = {
		cat  => sprintf( '%s/temp_%d_segments_%d_src.mkv', $last_dir, $main_pid, 1 ),
		cnt  => 1,
		dir  => $last_dir,
		dur  => $data->{duration},
		fps  => $data->{sourceFPS},
		idn  => sprintf( '%s/temp_%d_inter_dn_%d_%%d.mkv', $last_dir, $main_pid, 2 ),
		ids  => [$fileID],
		iup  => sprintf( '%s/temp_%d_inter_up_%d_%%d.mkv',    $last_dir, $main_pid, 3 ),
		lst  => sprintf( '%s/temp_%d_segments_%d_src.lst',    $last_dir, $main_pid, 4 ),
		prgu => sprintf( '%s/temp_%d_progress_up_%d_%%d.prg', $last_dir, $main_pid, 5 ),
		prgd => sprintf( '%s/temp_%d_progress_dn_%d_%%d.prg', $last_dir, $main_pid, 5 ),
		srcs => [$src],
		tmp  => sprintf( '%s/temp_%d_segments_%d_%%d.mkv', $last_dir, $main_pid, 6 )
	};

	return 1;
} ## end sub declare_single_source

# A die handler that lets perl death notes be printed via log
sub dieHandler {
	my ($err) = @_;

	$death_note = 5;
	$ret_global = 42;

	log_error( undef, '%s', $err );

	confess('Program died');
} ## end sub dieHandler

sub file_exists {
	my ($file) = @_;
	return ( defined($file) && ( ( length $file ) > 0 ) && ( -f $file ) ) ? 1 : 0;
}

sub format_bitrate {
	my ($float) = @_;
	return human_readable_size( $float, 0 ) . 'bits/s';
}

sub format_caller {
	my ($caller) = @_;
	$caller =~ s/^.*::([^:]+)$/$1/xms;
	$caller =~ m/__ANON__/xmsi and $caller = 'AnonSub';
	return $caller;
} ## end sub format_caller

sub format_out_time {
	my ($ms) = @_;
	my $sec  = floor( $ms / 1_000_000 );
	my $min  = floor( $sec / 60 );
	my $hr   = floor( $min / 60 );

	return sprintf '%02d:%02d:%02d.%06d', $hr, $min % 60, $sec % 60, $ms % 1_000_000;
} ## end sub format_out_time

sub get_info_from_ffprobe {
	my ( $src, $stream_fields ) = @_;
	my $avg_frame_rate_stream = -1;
	my @fpcmd                 = ( $FP, @FP_ARGS, "stream=$stream_fields", split( $SPACE, $source_info{$src}{probeStrings} ), $src );

	log_info( $work_data, 'Calling: %s', ( join $SPACE, @fpcmd ) );

	my @fplines = split /\n/ms, capture_cmd(@fpcmd);
	can_work or return 0;

	foreach my $line (@fplines) {
		chomp $line;

		log_debug( $work_data, 'RAW[%s]', $line );
		if ( $line =~ m/streams_stream_(\d)_([^=]+)="?([^"]+)"?/xms ) {
			$source_info{$src}{streams}[$1]{$2} = "$3";
			log_debug( $work_data, "    ==> Stream %d Field '%s' Value \"%s\"", $1, $2, $3 );
			if ( ( 'avg_frame_rate' eq $2 ) && ( '0' ne "$3" ) && ( '0/0' ne "$3" ) ) {
				my ( $s, $n, $v ) = ( $1, $2, $3 );

				# If we do not have found the stream defining our average framerate, yet, not it down now.
				( $avg_frame_rate_stream < 0 )
				  and log_debug( $work_data, '    ==> Avg Frame Rate Stream found!' )
				  and $avg_frame_rate_stream = $s;

				# If the framerate is noted as a fraction, calculate the numeric value
				$v =~ m/(\d+)\/(\d+)/ms
				  and ( 1. * $1 > 0. )
				  and ( 1. * $2 > 0. )
				  and $source_info{$src}{streams}[$s]{$n} = floor( 1. * ( ( 1. * $1 ) / ( 1. * $2 ) ) );
			} ## end if ( ( 'avg_frame_rate'...))
			next;
		} ## end if ( $line =~ m/streams_stream_(\d)_([^=]+)="?([^"]+)"?/xms)
		if ( $line =~ m/format_([^=]+)="?([^"]+)"?/xms ) {
			log_debug( $work_data, "    ==> Format Field '%s' Value \"%s\"", $1, $2 );
			$source_info{$src}{$1} = "$2";
		}
	} ## end foreach my $line (@fplines)

	return $avg_frame_rate_stream;
} ## end sub get_info_from_ffprobe

sub get_location {
	my ($data)           = @_;
	my $is_regular_log   = 0;
	my $curr_caller_line = ( caller 1 )[2] // -1;
	my $curr_caller_name = format_caller( ( caller 2 )[3] // 'main' );
	my $prev_caller_line = ( caller 2 )[2] // -1;
	my $prev_caller_name = format_caller( ( caller 3 )[3] // 'main' );

	( 'main::logMsg' eq ( caller 1 )[3] // 'undef' ) and check_logger($curr_caller_name) and $is_regular_log = 1;

	my $connect_string   = $EMPTY;
	my $line_format      = $EMPTY;
	my $pid_format       = $EMPTY;
	my $prev_info_format = $EMPTY;
	my @args             = ();

	if ( 1 == $is_regular_log ) {
		$pid_format  = '[%5d] ';
		$line_format = make_location_fmt( $data, 1, $prev_caller_line, ( length $prev_caller_name ) );

		# Curr is the logging function, prev is the function that does the logging
		push @args, $$;
		( $prev_caller_line > -1 ) and push @args, $prev_caller_line;
		push @args, $prev_caller_name;

	} else {
		$connect_string   = ' called from ';
		$line_format      = make_location_fmt( $data, 2, $curr_caller_line, ( length $curr_caller_name ) );
		$prev_info_format = make_location_fmt( $data, 3, $prev_caller_line, ( length $prev_caller_name ) );

		# curr is the function that calls the sub that logs, and prev is its caller
		( $curr_caller_line > -1 ) and push @args, $curr_caller_line;
		push @args, $curr_caller_name;
		( $prev_caller_line > -1 ) and push @args, $prev_caller_line;
		push @args, $prev_caller_name;
	} ## end else [ if ( 1 == $is_regular_log)]

	my $format_string = $pid_format . $line_format . $connect_string . $prev_info_format;

	return sprintf $format_string, @args;
} ## end sub get_location

sub get_log_level {
	my ($level) = @_;

	     ( $LOG_INFO == $level )    and return ('--Info--')
	  or ( $LOG_WARNING == $level ) and return ('Warning!')
	  or ( $LOG_ERROR == $level )   and return ('ERROR !!')
	  or ( $LOG_STATUS == $level )  and return ('-status-')
	  or return ('_DEBUG!_');

	return ('=DEBUG=');
} ## end sub get_log_level

sub get_pid_status {
	my ( $data, $pid ) = @_;

	( defined $data ) and ( defined $pid ) or return $FF_REAPED;

	my $is_locked = lock_data($data);
	my $status    = $data->{PIDs}{$pid}{status} // $FF_REAPED;
	( 1 == $is_locked ) and unlock_data($data);

	return $status;
} ## end sub get_pid_status

sub get_size_factor {
	my ($src) = @_;

	# Experiments showed, that the lower the bitrate of the source videos is, the more temporary space is needed.
	# This is mainly due to the usage of ut video, which means a pure set of iframes with bitrates going through
	# the roof.
	# Test videos went from a factor of ~20 with source bitrates around 180 mbit up to a factor of ~117 with
	# sources that had only 35 mbit.
	# We therefore use these two as boundaries, and the range in between is interpolated linear.
	my $mbit = ( $source_info{$src}{'bit_rate'} // 80_000_000 ) / 1024 / 1024;               ## MBit/s of the source file
	my $d1   = $mbit - 35;                                                                   ## Distance to lower bound
	my $f1   = $d1 / 145;                                                                    ## Amount of the lower bound factor
	my $d2   = 180 - $mbit;                                                                  ## Distance to upper bound
	my $f2   = $d2 / 145;                                                                    ## Amount of the upper bound factor
	my $res  = ( $mbit <= 45 ) ? 100 : ( $mbit >= 180 ) ? 20 : ( $f1 * 120 ) + ( $f2 * 20 );

	log_debug( $work_data, "File '%s': %d lower distance, %d upper distance", $src, $d1, $d2 );
	log_debug( $work_data, '  => %3.2f lower factor, %3.2f upper factor',     $f1,  $f2 );
	log_debug( $work_data, '  => %3.2f Final factor',                         $res );

	return $res;
} ## end sub get_size_factor

sub get_time_now {
	my @tLocalTime = localtime;
	return sprintf '%04d-%02d-%02d %02d:%02d:%02d', $tLocalTime[5] + 1900, $tLocalTime[4] + 1, $tLocalTime[3], $tLocalTime[2], $tLocalTime[1], $tLocalTime[0];
}

sub handle_eval_result {
	my ( $fork_data, $res, $eval_err, $child_error, $p_exit_code, $p_exit_message ) = @_;

	log_debug( $fork_data, "Handling eval result res %d, err '%s', chld %d, exc %d, exm '%s'", $res, $eval_err, $child_error, ${$p_exit_code}, ${$p_exit_message} );

	if ( length($eval_err) > 0 ) {
		( 0 == ${$p_exit_code} ) and ${$p_exit_code} = -1;
		${$p_exit_message} = $eval_err;
		log_debug( $fork_data, "eval failed with error message '%s' [-1]", $eval_err );
		$res = 0;
	} elsif ( -1 != $child_error ) {
		if ( $child_error & 0x7F ) {
			( 0 == ${$p_exit_code} ) and ${$p_exit_code} = $child_error;
			${$p_exit_message} = 'Killed by signal ' . ( $child_error & 0x7F );
			$res = 0;
			log_debug( $fork_data, 'eval killed by signal %d [%d]', ( $child_error & 0x7F ), $child_error );
		} elsif ( $child_error >> 8 ) {
			( 0 == ${$p_exit_code} ) and ${$p_exit_code} = $child_error >> 8;
			${$p_exit_message} = 'Exited with error ' . ( $child_error >> 8 );
			$res = 0;
			log_debug( $fork_data, 'eval exited with error %d [%d]', ( $child_error >> 8 ), $child_error );
		} else {
			log_debug( $fork_data, 'eval exited cleanly, child errno is %d', $child_error );
			$res = 1;
		}
	} ## end elsif ( -1 != $child_error)

	return $res;
} ## end sub handle_eval_result

sub handle_fork_message {
	my ($errmsg) = @_;

	my @messages   = split /\n/ms, $errmsg;
	my $have_error = 0;

	for my $msg (@messages) {
		chomp $msg;
		my $is_error   = ( $msg =~ m/(error|critical)/ims ) ? 1 : 0;
		my $is_warning = ( $msg =~ m/warning/ims )          ? 1 : 0;
		my $is_info    = ( $msg =~ m/(info|status)/ims )    ? 1 : 0;

		$is_error        and log_error( $work_data, $msg ) and ( $have_error = 1 )
		  or $is_warning and log_warning( $work_data, $msg )
		  or $is_info    and log_info( $work_data, $msg );
	} ## end for my $msg (@messages)

	return $have_error;
} ## end sub handle_fork_message

##
# @brief    Handles the progress of a fork, loading its current values into @a $prgData
# @details  This function is used to handle the progress of a particular fork referred by its PID.
#           It also resets the timeout counter based on the load progress. If a fork appears to be frozen,
#           a warning message is triggered.
#
# @param    $pid The PID of the fork.
# @param    $prgData The progress file last block is loaded into this hashref.
# @param    $fork_timeout Timeout object for forks.
# @return   Returns -1 if the PID is gone without any progress, 0 if progress has ended successfully and 1 if it is still running.
sub handle_fork_progress {
	my ( $pid, $prgData, $fork_timeout ) = @_;
	my $pidstat        = is_pid_active($pid);
	my $prgfile        = $work_data->{PIDs}{$pid}{prgfile} // $EMPTY;
	my $progress_state = $PROGRESS_NONE;

	# is_pid_active() returns -1 on PID crash/missing and 1 if the PID is active, so just check >0.
	my $result = $pidstat > 0;

	if ( ( 0 < ( length $prgfile ) ) && ( -f $prgfile ) ) {
		log_debug( $work_data, "Loading Progress PID %d, File '%s' [%d/%d]", $pid, $work_data->{PIDs}{$pid}{prgfile}, $pidstat, $result );
		$progress_state = load_progress( $pid, $work_data->{PIDs}{$pid}{prgfile}, $prgData );

		# If the PID has just been terminated due to a frozen sub process, it now looks quite nicely ended,
		# and a terminated ffmpeg will have written so in its progress log ("progress=end" as the final line)
		# Therefore the progress state has to be overridden if we just have killed this PID
		if ( ( $FF_RUNNING < get_pid_status( $work_data, $pid ) ) && pid_shall_restart( $work_data, $pid ) ) {
			$progress_state = $PROGRESS_NONE;
		}
	} ## end if ( ( 0 < ( length $prgfile...)))

	# Now handle timeouts according to the actual progress state
	( $PROGRESS_NONE == $progress_state )     and ( $result > 0 ) and --$fork_timeout->{$pid};
	( $PROGRESS_CONTINUE == $progress_state ) and $fork_timeout->{$pid} = $TIMEOUT_INTERVALS;
	( $PROGRESS_ENDED == $progress_state )    and $fork_timeout->{$pid} = $TIMEOUT_INTERVALS and $result = 0;

	# Warn if a fork looks like it is freezing...
	( $result > 0 ) and ( ( $TIMEOUT_INTERVALS / 2 ) == $fork_timeout->{$pid} ) and log_warning( $work_data, 'Fork PID %d seems to be frozen...', $pid );

	# if th epid is gone, only return -1 (has crashed, program will be torn down) if the PID is not marked for restart already.
	return ( $pidstat >= 0 ) ? $result : pid_shall_restart( $work_data, $pid ) ? 0 : -1;
} ## end sub handle_fork_progress

sub handle_fork_strikes {
	my ( $pid, $fork_timeout, $fork_strikes ) = @_;

	defined( $fork_timeout->{$pid} ) or $fork_timeout->{$pid} = $TIMEOUT_INTERVALS;
	defined( $fork_strikes->{$pid} ) or $fork_strikes->{$pid} = 0;

	if (   ( $fork_timeout->{$pid} <= 0 )
		&& ( pid_shall_restart( $work_data, $pid ) || ( get_pid_status( $work_data, $pid ) < $FF_FINISHED ) ) )
	{
		++$fork_strikes->{$pid};
		$fork_strikes->{$pid} =
		    ( $fork_strikes->{$pid} == 13 ) ? strike_fork_reap($pid)
		  : ( $fork_strikes->{$pid} == 7 )  ? strike_fork_kill($pid)
		  : ( $fork_strikes->{$pid} == 1 )  ? strike_fork_term($pid)
		  :                                   $fork_strikes->{$pid};
		if ( $fork_strikes->{$pid} > 17 ) {
			my $kid = strike_fork_restart($pid);
			$fork_timeout->{$kid} = $TIMEOUT_INTERVALS;
			$fork_strikes->{$kid} = 0;
			log_warning( $work_data, 'Worker PID %d substituted PID %d', $kid, $pid );
		} ## end if ( $fork_strikes->{$pid...})
	} ## end if ( ( $fork_timeout->...))

	return 1;
} ## end sub handle_fork_strikes

sub handle_io_operations {
	my (
		#@type IO::Select
		$io_selector,
		$fh_err, $msg_out_p, $msg_err_p
	) = @_;

	my $are_ready  = 1;
	my $have_lines = 1;

	while ( ( $are_ready > 0 ) && ( $have_lines > 0 ) && ( my @ready = $io_selector->can_read(0.1) ) ) {
		$are_ready  = scalar @ready;
		$have_lines = 0;

		for my $handle (@ready) {
			( defined $handle ) or next;
			my $line = <$handle>;

			if ( defined $line ) {
				chomp $line;
				( $handle == $fh_err ) and ( push @{$msg_err_p}, $line ) or ( push @{$msg_out_p}, $line );
				++$have_lines;
			}
		} ## end for my $handle (@ready)
		usleep(5_000);
	} ## end while ( ( $are_ready > 0 ...))

	return 1;
} ## end sub handle_io_operations

# React on getting a termination request from the main program by signal or by DEATH counter
sub handle_termination_request {
	my ( $fork_data, $cmd_pid ) = @_;

	( $$ != $main_pid ) or confess("FATAL: handle_termination_request() called from MAIN PID $main_pid instead from a fork !");

	# Transfer DEATH from main program if set
	lock_data($fork_data);
	( $fork_data->{DEATH} > $death_note ) and $death_note = $fork_data->{DEATH};
	unlock_data($fork_data);

	if ( $death_note > 0 ) {
		( $death_note < 4 ) and log_debug( $fork_data, "Sending 'TERM' to PID %d", $cmd_pid ) and ( kill 'TERM', $cmd_pid );

		# react on 4, because on 5 this fork will be killed (See terminator())
		( 3 < $death_note ) and log_debug( $fork_data, "Sending 'KILL' to PID %d", $cmd_pid ) and ( kill 'KILL', $cmd_pid );

		# In any way, sleep 180ms, so the wait in run:_cmd_from_fork() reaches 200ms.
		usleep(180_000);
	} ## end if ( $death_note > 0 )

	return 1;
} ## end sub handle_termination_request

sub human_readable_size {
	my ( $num, $is_byte ) = @_;
	my $int  = floor($num);
	my @exps = qw( B K M G T P E Z );
	my $exp  = 0;

	while ( $int >= 1024 ) {
		++$exp;
		$int /= 1024;
	}

	return sprintf '%3.2f %s', floor( $int * 100. ) / 100., $is_byte ? $exps[$exp] : $exp > 0 ? lc $exps[$exp] : $EMPTY;
} ## end sub human_readable_size

sub initialize_fork_watch {
	my ( $fork_strikes, $fork_timeout, @PIDs ) = @_;
	my $forks_active = 0;

	# Initialize timeout, strike data and fork count
	foreach my $pid (@PIDs) {
		$fork_timeout->{$pid} = $TIMEOUT_INTERVALS;
		$fork_strikes->{$pid} = 0;
		is_pid_active($pid) and ++$forks_active;
	}

	# Let's do a little check to see whether everything is set up properly
	if ( $work_data->{cnt} != $forks_active ) {
		log_warning( $work_data, '%d forks should be active, but %d are!', $work_data->{cnt}, $forks_active );
		( $work_data->{cnt} > $forks_active ) and $forks_active = $work_data->{cnt};
	}

	return $forks_active;
} ## end sub initialize_fork_watch

sub interpolate_source_group {
	my ( $gid, $inter_opts ) = @_;
	can_work() or return 1;

	if ( !( defined $source_groups{$gid} ) ) {
		log_error( $work_data, 'Source Group ID %d does not exist!', $gid );
		return 0;
	}

	# Building the worker fork is quite trivial
	for ( 0 .. 3 ) {
		can_work()                                 or return 1;
		start_worker_fork( $_, $gid, $inter_opts ) or return 0;
	}

	# Watch and join
	return watch_my_forks();
} ## end sub interpolate_source_group

# @brief Check whether a PID is active
# @return -1 if the PID is no longer listed or reap_pid() detected a crashed PID, 1 if the PID is working and 0 if has ended.
sub is_pid_active {
	my ($pid)       = @_;
	my $data_exists = pid_exists( $work_data, $pid );
	my $reap_status = reap_pid($pid);

	# reap_pid() returns PID if the PID is defined and the child process has finished.
	# If the PID was not found, -1 is returned. 0 is returned if the PID is still busy.
	# Therefore $pid_exists is 1 if $reap_status is 0, because it means reap_pid() found it to be active.
	my $pid_exists = $reap_status < 0 ? -1 : $reap_status > 0 ? 0 : 1;
	return $data_exists ? $pid_exists : -1;
} ## end sub is_pid_active

sub is_progress_line {
	my ($line) = @_;
	return $line =~ m/^progress=/xms;
}

# Load data from between the last two "progress=<state>" lines in the given log file, and store it in the given hash
# If the hash has values, progress data is added.
sub load_progress {
	my ( $pid, $progress_log, $progress_data ) = @_;

	file_exists($progress_log) or return $PROGRESS_NONE;

	my @last_20_lines = read_and_reverse_last_lines( $progress_log, 20 );
	my $lines_count   = scalar @last_20_lines;

	my $progress_count = 0;
	my $progress_state = $PROGRESS_NONE;
	my $i              = 0;
	while ( ( $progress_count < 1 ) && ( $i < $lines_count ) ) {
		chomp $last_20_lines[$i];
		log_debug( $work_data, "[%d line % 2d] Check '%s'", $pid, $i, $last_20_lines[$i] );
		if ( $last_20_lines[$i] =~ m/^progress=(\S+)/xms ) {

			# As we parse backwards, this is the actual state of the process
			++$progress_count;
			$progress_state = ( 'continue' eq $1 ) ? $PROGRESS_CONTINUE : ( 'end' eq $1 ) ? $PROGRESS_ENDED : $PROGRESS_NONE;
		} ## end if ( $last_20_lines[$i...])
		$i++;
	} ## end while ( ( $progress_count...))

	my @progress_field_names = qw( bitrate drop_frames dup_frames fps frame out_time_ms total_size );
	while ( ( $progress_count < 2 ) && ( $i < $lines_count ) ) {
		chomp $last_20_lines[$i];
		log_debug( $work_data, "[%d line % 2d] Check '%s'", $pid, $i, $last_20_lines[$i] );
		if ( is_progress_line( $last_20_lines[$i] ) ) {
			$progress_count++;
		} else {
			foreach (@progress_field_names) {
				parse_progress_data( $last_20_lines[$i], $_, $progress_data ) and last;
			}
		}
		$i++;
	} ## end while ( ( $progress_count...))
	return $progress_state;
} ## end sub load_progress

sub lock_data {
	my ($data) = @_;
	my $result = 1;

	( defined $data ) or return 0;

	#@type IPC::Shareable
	my $lock = tied %{$data};

	my $stLoc = get_location(undef);

	( $do_lock_debug > 0 ) and log_debug( $work_data, '%s try lock ...', $stLoc );
	( defined $lock ) and ( $result = $lock->lock(LOCK_EX) ) or $result = 0;
	( $do_lock_debug > 0 ) and log_debug( $work_data, '%s ==> LOCK [%d]', $stLoc, $result // 'undef' );

	return $result // 0;
} ## end sub lock_data

sub logMsg {
	my ( $data, $lvl, $fmt, @args ) = @_;

	( defined $lvl ) or $lvl = 2;

	( $LOG_DEBUG == $lvl ) and ( 0 == $do_debug ) and return 1;

	if ( !( defined $fmt ) ) {
		$fmt = shift @args // $EMPTY;
	}

	# If $fmt is now a fixed string, and @args is empty, we have to make sure that all
	# possible formatting strings are ignored, as the string might come from an error
	# handler.
	if ( 0 == scalar @args ) {
		@args = ($fmt);  ## Make the fixed string the first (and only) argument
		$fmt  = '%s';    ## And print it "as-is".
	}

	my $stTime  = get_time_now();
	my $stLevel = get_log_level($lvl);
	my $stMsg   = sprintf "%s|%s|%s|$fmt", $stTime, $stLevel, get_location($data), @args;

	( 0 < ( length $logfile ) ) and write_to_log($stMsg);
	( $LOG_INFO < $lvl ) and write_to_console($stMsg);

	return 1;
} ## end sub logMsg

sub log_info {
	my ( $data, $fmt, @args ) = @_;
	return logMsg( $data, $LOG_INFO, $fmt, @args );
}

sub log_warning {
	my ( $data, $fmt, @args ) = @_;
	return logMsg( $data, $LOG_WARNING, $fmt, @args );
}

sub log_error {
	my ( $data, $fmt, @args ) = @_;
	$ret_global = 1;
	return logMsg( $data, $LOG_ERROR, $fmt, @args );
}

sub log_status {
	my ( $data, $fmt, @args ) = @_;
	return logMsg( $data, $LOG_STATUS, $fmt, @args );
}

sub log_debug {
	my ( $data, $fmt, @args ) = @_;
	$do_debug or return 1;
	return logMsg( $data, $LOG_DEBUG, $fmt, @args );
}

sub make_filter_string {
	my ( $gid, $inter_opts ) = @_;
	my $do_alt   = $inter_opts->{'do_alt'};
	my $dropdups = $gid >= 0 ? $source_groups{$gid}{dropdups} // 0 : 0;
	my $dec_max  = $inter_opts->{'dec_max'};
	my $dec_frac = $inter_opts->{'dec_frac'};
	my $src_fps  = $gid >= 0 ? $source_groups{$gid}{fps} // $target_fps : $target_fps;
	my $tgt      = $inter_opts->{'tgt'};
	my $tgt_fps  = $inter_opts->{'fps'};
	( defined $do_alt ) and ( ( 0 == $do_alt ) or ( 1 == $do_alt ) ) or confess("do_alt $do_alt out of range! (0/1)");
	can_work()                                                       or return 1;

	# Prepare filter components
	my $F_in_scale   = "scale='in_range=full:out_range=full'";
	my $F_mpdecimate = "mpdecimate='max=${dec_max}:frac=${dec_frac}'";
	my $F_out_scale  = "scale='flags=accurate_rnd+full_chroma_inp+full_chroma_int:in_range=full:out_range=full'";
	my $F_interpolate;

	if ( 'iup' eq $tgt ) {

		# If we have a source with more FPS than our max_fps, libplacebo must interpolate to get the timings right.
		# Otherwise libplacebo can use a none interpolation. minterpolate is always using simple dup, no matter what.
		$F_interpolate = sprintf 0 == $do_alt ? ( $src_fps > $tgt_fps ? $ff_interp_libp_high : $ff_interp_libp_none ) : $ff_interp_mint_none, $tgt_fps;
	} elsif ( 'idn' eq $tgt ) {

		# When calculating down to the target FPS, we always use high interpolation.
		# But on alternative interpolation we only use high minterpolate if there actually are dropped/dupped frames
		$F_interpolate = sprintf 0 == $do_alt ? $ff_interp_libp_high : $ff_interp_mint_high, $tgt_fps;
	} else {
		my $interpolation_type;

		# This is the last step, the creation of the target video.
		# If there wasn't any dropped frames, use the simple minterpolate if libplacebo freezes.
		# This latest mpdecimate/minterpolate only happen, to solve lengthier lag parts, that aren't fully solved by the up.-/down-scaling.
		if ( 0 == $dropdups ) {
			$interpolation_type = ( 0 == $do_alt ) ? $ff_interp_libp_none : $ff_interp_mint_none;
		} else {
			$interpolation_type = ( 0 == $do_alt ) ? $ff_interp_libp_high : $ff_interp_mint_high;
		}

		$F_interpolate = sprintf $interpolation_type, $tgt_fps;

		# Here we also need an fps filter, the output will be cfr anyway.
		$F_mpdecimate = "fps=fps=$target_fps:round=near,$F_mpdecimate";
	} ## end else [ if ( 'iup' eq $tgt ) ]

	return "pad=ceil(iw/2)*2:ceil(ih/2)*2,${F_in_scale}${B_decimate}${F_mpdecimate}${B_middle}${F_out_scale}${B_interp}${F_interpolate}";
} ## end sub make_filter_string

sub make_location_fmt {
	my ( $data, $idx, $lineno, $name_len ) = @_;

	if ( defined $data ) {
		( $name_len > $data->{MLEN}[$idx] ) and ( $data->{MLEN}[$idx] = $name_len ) and $data->{ULEN}[$idx] = 0;
		( $name_len < $data->{MLEN}[$idx] ) and ( ++$data->{ULEN}[$idx] ) or $data->{ULEN}[$idx] = 0;
		( $data->{ULEN}[$idx] >= 10 )       and ( $data->{MLEN}[$idx]-- ) and $data->{ULEN}[$idx] = 0;
	}
	my $len = ( ( defined $data ) ? $data->{MLEN}[$idx] : $name_len ) + ( ( $lineno > -1 ) ? 0 : 5 );

	my $fmtfmt = ( $lineno > -1 ) ? '%%4d:%%-%ds' : '%%-%ds';

	return sprintf $fmtfmt, $len;
} ## end sub make_location_fmt

sub mark_pid_restart {
	my ($pid) = @_;

	lock_data($work_data);
	if ( !( defined $work_data->{RESTART}{$pid} ) || ( $work_data->{RESTART}{$pid} < 1 ) ) {
		$work_data->{RESTART}{$pid} = 1;
		log_debug( $work_data, 'PID %5d marked for restart', $pid );
	}
	unlock_data($work_data);

	return 1;
} ## end sub mark_pid_restart

sub parse_progress_data {
	my ( $line, $property_name, $data ) = @_;

	# Attempt 1: Simple floating point value
	if ( $line =~ m/^${property_name}="?([.0-9]+)"?\s*$/xms ) {
		log_debug( $work_data, "${EIGHTSPACE}==> %s=%f", $property_name, $1 );
		$data->{$property_name} += ( 1 * $1 );
		return 1;
	}

	# Attempt 2: bitrate
	if ( $line =~ m/^${property_name}="?([.0-9]+)(.)b?its?\/s"?\s*$/xms ) {
		my $bits = 1.0 * $1;
		my $exp  = lc $2;
		log_debug( $work_data, "${EIGHTSPACE}==> %s=%f %sbits/s", $property_name, $bits, $exp );
		( 'g' eq $exp ) and $bits *= 1024 and $exp = 'm';
		( 'm' eq $exp ) and $bits *= 1024 and $exp = 'k';
		( 'k' eq $exp ) and $bits *= 1024 and $exp = $EMPTY;
		log_debug( $work_data, "${EIGHTSPACE}==> %s=%f %sbits/s", $property_name, $bits, $exp );
		$data->{$property_name} += ( 1 * $bits );
		return 1;
	} ## end if ( $line =~ m/^${property_name}="?([.0-9]+)(.)b?its?\/s"?\s*$/xms)

	return 0;
} ## end sub parse_progress_data

sub pid_exists {
	my ( $data, $pid ) = @_;
	lock_data($data);
	my $exists = defined( $data->{PIDs}{$pid} );
	unlock_data($data);
	return $exists;
} ## end sub pid_exists

sub pid_count_dec {
	my $stLoc = get_location(undef);

	lock_data($work_data);
	my $new_count = ( $work_data->{cnt} // 0 ) - 1;
	( $new_count >= 0 ) or $new_count = 0;
	log_debug( $work_data, 'Decreasing PID count from %d to %d (%s)', $work_data->{cnt} // 0, $new_count, $stLoc );
	$work_data->{cnt} = $new_count;
	unlock_data($work_data);

	return 1;
} ## end sub pid_count_dec

sub pid_count_inc {
	my $stLoc = get_location(undef);

	lock_data($work_data);
	my $new_count = ( $work_data->{cnt} // 0 ) + 1;
	( $new_count > 0 ) or $new_count = 1;
	log_debug( $work_data, 'Increasing PID count from %d to %d (%s)', $work_data->{cnt} // 0, $new_count, $stLoc );
	$work_data->{cnt} = $new_count;
	unlock_data($work_data);

	return 1;
} ## end sub pid_count_inc

sub pid_shall_restart {
	my ( $data, $pid ) = @_;

	( defined $data ) and ( defined $pid ) or return 1;

	my $is_locked = lock_data($data);
	my $result    = $work_data->{RESTART}{$pid} // 0;
	( 1 == $is_locked ) and unlock_data($data);

	return $result;
} ## end sub pid_shall_restart

sub pid_status_to_str {
	my ($status) = @_;

	return
	    ( $FF_REAPED == $status )   ? 'REAPED'
	  : ( $FF_FINISHED == $status ) ? 'finished'
	  : ( $FF_KILLED == $status )   ? 'KILLED'
	  : ( $FF_RUNNING == $status )  ? 'running'
	  : ( $FF_CREATED == $status )  ? 'created'
	  :                               '***unknown***';
} ## end sub pid_status_to_str

sub plural_s {
	my ($num) = @_;

	if ( $num =~ m/^(\d+)$/xms ) {
		( 1 != $1 ) and return 's';
	}

	return $EMPTY;
} ## end sub plural_s

sub read_and_reverse_last_lines {
	my ( $filename, $linecount ) = @_;

	my $file   = File::ReadBackwards->new($filename) or croak("Cannot open $filename: $!");
	my @lines  = ();
	my $lineno = 0;

	while ( ( $lineno++ < $linecount ) && ( my $line = $file->readline() ) ) {
		chomp $line;
		push @lines, $line;
	}

	return @lines;
} ## end sub read_and_reverse_last_lines

##
# @brief Reap a child process by its process ID (PID).
#
# @details The reap_pid subroutine checks if a child process related to
# the provided PID has finished its job or not. If finished, it sets the
# status of the process to 'reaped'. If still busy, it returns 0.
#
# @param[in] $pid The Process ID of child process to be reaped.
#
# @return Returns PID if the PID is defined and the child process has finished.
# If the PID was not found, -1 is returned. 0 is returned if the PID is still busy.
#
# @note It also reports an error and aborts if the provided PID is not
# valid or not defined.
sub reap_pid {
	my ($pid) = @_;

	( defined $pid ) and ( $pid =~ m/^\d+$/ms )
	  or log_error( $work_data, q{reap_pid(): BUG! '%s' is not a valid pid!}, $pid // 'undef' )
	  and confess('FATAL BUG!');
	defined( $work_data->{PIDs}{$pid} ) or return 1;
	( $FF_REAPED == get_pid_status( $work_data, $pid ) ) and return 1;

	my $pidstat = waitpid $pid, POSIX::WNOHANG;
	( 0 == $pidstat ) and return 0;  ## PID is still busy!

	log_debug( $work_data, '(reap_pid) KID %d %s', $pid, ( $pidstat < 0 ) ? 'is already gone' : 'has been ended' );
	set_pid_status( $work_data, $pid, $FF_REAPED );
	log_debug( $work_data, '(reap_pid) KID %d status set to %s', $pid, pid_status_to_str( get_pid_status( $work_data, $pid ) ) );

	return $pidstat;
} ## end sub reap_pid

# ---------------------------------------------------------
# REAPER function - $SIG{CHLD} exit handler
# ---------------------------------------------------------
sub reaper {
	my @args = @_;

	while ( ( my $pid = ( waitpid -1, POSIX::WNOHANG ) ) > 0 ) {
		log_debug( undef, '(reaper) KID %d finished [%s]', $pid, ( join q{,}, @args ) );
		( defined $work_data ) and $work_data->{PIDs}{$pid}{status} = $FF_REAPED;
		log_debug( undef, '(reaper) KID %d status set to %s', $pid, pid_status_to_str( $work_data->{PIDs}{$pid}{status} // -1 ) );
		## Note: Do not try to lock within a signal handler!
	} ## end while ( ( my $pid = ( waitpid...)))

	$SIG{CHLD} = \&reaper;

	return 1;
} ## end sub reaper

sub remove_pid {
	my ( $pid, $do_cleanup ) = @_;
	my $result       = 1;
	my $is_restarted = pid_shall_restart( $work_data, $pid );

	check_pid($pid) or return 1;

	while ( 0 == reap_pid($pid) ) {
		usleep(250_000);  ## For times a second is enough
	}

	( 1 == $do_cleanup ) and $result = cleanup_pid($pid);

	lock_data($work_data);

	# Progress files are always removed, because the only part where they are used
	# will no longer pick them up once the PID was removed from %work_data (See watch_my_forks())
	# However, if the PID got restarted, the new might have already started a new progress file, so
	# leave it alone in that case. The old one was deleted then anyway.
	my $prgfile = $work_data->{PIDs}{$pid}{prgfile} // $EMPTY;  ## shortcut including (defined check)
	if ( ( length($prgfile) > 0 ) && ( -f $prgfile ) && ( 0 == $is_restarted ) ) {
		( $do_debug > 0 ) and ( 0 == $is_restarted ) and log_debug( $work_data, 'See: %s', $prgfile ) or unlink $prgfile;
	}

	delete( $work_data->{RESTART}{$pid} );
	delete( $work_data->{PIDs}{$pid} );
	unlock_data($work_data);

	pid_count_dec();

	return $result;
} ## end sub remove_pid

sub run_cmd_from_fork {
	my ( $fork_data, $cmd, $msg_out_p, $msg_err_p, $exc_p, $exm_p ) = @_;
	my $chld_error = 0;

	my $res = eval {

		# Catch signals within the fork, too.
		local $SIG{INT}  = \&sigHandler;
		local $SIG{QUIT} = \&sigHandler;
		local $SIG{TERM} = \&sigHandler;

		# But ignore SIGCHLD, we have to reap the started process ourself
		local $SIG{CHLD} = 'IGNORE';
		my $cmd_pid = open3( undef, my $stdout, my $stderr = gensym, @{$cmd} );

		#@type IO::Select
		my $io_selector = IO::Select->new();
		$io_selector->add($stdout);
		$io_selector->add($stderr);

		log_debug( $fork_data, "Started '%s' with PID %d", $cmd->[0], $cmd_pid );

		while ( 0 == ( waitpid $cmd_pid, POSIX::WNOHANG ) ) {

			# Handle ready IO operations
			handle_io_operations( $io_selector, $stderr, $msg_out_p, $msg_err_p );

			# React on external wishes to end this fork
			handle_termination_request( $fork_data, $cmd_pid );
			usleep(20_000);
		} ## end while ( 0 == ( waitpid $cmd_pid...))

		$chld_error = ( $? > 0 ) ? $? : 0;  ## -1 means, that waitpid has not have had a process to reap. Not an error!
		${$exc_p} = $chld_error >> 8;

		# Before we can leave, make sure no lingering output is in the pipeline
		handle_io_operations( $io_selector, $stderr, $msg_out_p, $msg_err_p );

		# Now everything is sqeaky clean.
		$io_selector->remove($stderr);
		$io_selector->remove($stdout);

		log_debug( $fork_data, "'%s' PID %d %s", $cmd->[0], $cmd_pid, select_termination_message() );
	};
	$res = handle_eval_result( $fork_data, $res, $@, $chld_error, $exc_p, $exm_p );

	return $res;
} ## end sub run_cmd_from_fork

sub select_termination_message {
	return (
		  ( $death_note < 0 ) ? 'frozen'
		: ( $death_note < 1 ) ? 'ended'
		: ( $death_note < 4 ) ? 'terminated'
		:                       'killed'
	);
} ## end sub select_termination_message

sub segment_all_groups {
	can_work() or return 1;

	log_status( $work_data, 'Segmenting source groups...' );

	foreach my $groupID ( sort { $a <=> $b } keys %source_groups ) {
		can_work() or last;
		my $prgfile_fmt = sprintf '%s/temp_%d_progress_cc_%d_%%d.prg', $source_groups{$groupID}{dir}, $main_pid, $groupID;
		segment_source_group( $groupID, $prgfile_fmt ) or exit 8;
	}

	return 1;
} ## end sub segment_all_groups

sub segment_source_group {
	my ( $gid, $prgfile_fmt ) = @_;
	my $result = 1;
	( defined $source_groups{$gid} ) or log_error( $work_data, 'Source Group ID %d does not exist!', $gid ) and return 0;
	can_work()                       or return 1;

	# We use this to check on the overall maximum fps
	( $source_groups{$gid}{fps} > $max_fps ) and $max_fps = $source_groups{$gid}{fps};

	# Each segment must be a quarter of the total duration, raised to the next full second
	my $seg_len = floor( 1. + ( $source_groups{$gid}{dur} / 4. ) );

	# Before we can segment the video, we have to concatenate associated parts.
	# If we do it in one go, some segments might be able to freeze ffmpeg later.
	if ( $source_groups{$gid}{cnt} > 1 ) {
		log_status( $work_data, 'Concatenating %d source files...', $source_groups{$gid}{cnt} );
		$result = concat_source_group( $gid, $prgfile_fmt );
		( 1 == $result ) or return $result;
		can_work()       or return 1;
		log_status( $work_data, 'Segmenting concatenated file...' );
	} ## end if ( $source_groups{$gid...})

	# If there is only one source file, we do not have a concatenated one now, so use the source directly.
	else {
		$source_groups{$gid}{cat} = abs_path( $source_groups{$gid}{srcs}[0] );
	}

	# The segmentations is rather simple, just copy the source and add the segment format
	# Let's build the command line arguments:
	my $prgfile = sprintf $prgfile_fmt, 2;
	my @ffargs  = (
		$FF,  @FF_ARGS_START, '-progress', $prgfile, ( ( 'guess' ne $audio_layout ) ? qw( -guess_layout_max 0 ) : () ),
		'-i', $source_groups{$gid}{cat},
		@FF_SEGMENT_BEGIN, "$seg_len", @FF_SEGMENT_END, $source_groups{$gid}{tmp}
	);

	log_info( $work_data, "Starting Worker %d for:\n%s", 1, ( join $SPACE, @ffargs ) );
	my $pid = start_work( 1, $gid, @ffargs );
	( defined $pid ) and ( $pid > 0 ) or croak('BUG! start_work() returned invalid PID!');
	lock_data($work_data);
	@{ $work_data->{PIDs}{$pid}{args} } = @ffargs;
	$work_data->{PIDs}{$pid}{prgfile} = $prgfile;
	unlock_data($work_data);

	# Watch and join
	$result = watch_my_forks();

	# The concatenation file is no longer needed.
	( $source_groups{$gid}{cnt} > 1 ) and ( -f $source_groups{$gid}{cat} ) and ( 0 == $do_debug ) and unlink $source_groups{$gid}{cat};

	# The progress file can go, too
	-f $prgfile and ( 0 == $do_debug ) and unlink $prgfile;

	return $result;
} ## end sub segment_source_group

sub send_forks_the_kill() {

	# Everytime this subroutine is called, it raises the death_note.
	# The plan is, that it is called every half second from watch_my_forks() if can_work() returns false
	# So do not call it from anywhere else, and do not manipulate $death_note from anywhere else
	++$death_note;

	# Ensure that global termination requests get transferred next
	update_termination_request();

	lock_data($work_data);
	my @PIDs = keys %{ $work_data->{PIDs} };
	unlock_data($work_data);

	foreach my $pid (@PIDs) {
		if ( ( 1 == $death_note ) && ( $FF_REAPED != get_pid_status( $work_data, $pid ) ) ) {
			log_warning( $work_data, 'TERMing worker PID %d', $pid );
			terminator( $pid, 'TERM' );
		}

		# Note: 5 is after 2 seconds
		elsif ( ( 4 < $death_note ) && ( $FF_REAPED != get_pid_status( $work_data, $pid ) ) ) {
			log_warning( $work_data, 'KILLing worker PID %d', $pid );
			terminator( $pid, 'KILL' );
		}
	} ## end foreach my $pid (@PIDs)

	return 1;
} ## end sub send_forks_the_kill

sub set_log_file {
	$logfile = $path_target;
	$logfile =~ s/[.][^.]+$/.log/ms;
	return 1;
}

sub set_pid_status {
	my ( $data, $pid, $status ) = @_;

	( defined $data ) and ( defined $pid ) or return 1;

	lock_data($data);
	( defined $data ) and $data->{PIDs}{$pid}{status} = $status;
	log_debug( $work_data, 'PID %5d = %s', $pid, pid_status_to_str($status) );
	unlock_data($work_data);

	return 1;
} ## end sub set_pid_status

# Show data from between the last two "progress=<state>" lines in the given log file
sub show_progress {
	my ( $thr_count, $thr_active, $prgData, $log_as_status ) = @_;

	( $thr_count > 0 ) or $thr_count = 1;

	# qw( bitrate drop_frames dup_frames fps frame out_time_ms total_size )

	# Formualate the progress line
	my $size_str    = human_readable_size( $prgData->{total_size} // 0, 1 );
	my $time_str    = format_out_time( $prgData->{out_time_ms}    // 0 );
	my $bitrate_str = format_bitrate( ( $prgData->{bitrate} // 0.0 ) / $thr_count );  ## Average, not the sum.
	my $progress_str =
	  ( $prgData->{frame} > 0 )
	  ? (
		sprintf '[%d/%d running] Frame %d (%d drp, %d dup); %s; FPS: %03.2f; %s; File Size: %s    ',
		$thr_active, $thr_count, $prgData->{frame},
		$prgData->{drop_frames},
		$prgData->{dup_frames},
		$time_str, $prgData->{fps}, $bitrate_str, $size_str
	  )
	  : ( sprintf '[%d/%d running] %s    ', $thr_active, $thr_count, $time_str );

	# Clear a previous progress line
	( $have_progress_msg > 0 ) and print "\r" . ( $SPACE x length $progress_str ) . "\r";

	if ( 0 < $log_as_status ) {

		# Write into log file
		$have_progress_msg = 0;  ## ( We already deleted the line above, leaving it at 1 would add a useless empty line. )
		( $prgData->{frame} > 0 ) and log_status(
			$work_data, "%d fork%s finished after %d frames, duration %s, FPS %03.2f, %s, file size %s\n" . '    (%d frames dropped, %d frames duplicated)',
			$thr_count, plural_s($thr_count), $prgData->{frame}, $time_str, $prgData->{fps}, $bitrate_str, $size_str,
			$prgData->{drop_frames},
			$prgData->{dup_frames}
		) or log_status( $work_data, '%d fork%s finished, total duration is: %s', $thr_count, plural_s($thr_count), $time_str );
	} else {

		# Output on console
		$have_progress_msg = 1;
		local $| = 1;
		print "\r${progress_str}";
	} ## end else [ if ( 0 < $log_as_status)]

	return 1;
} ## end sub show_progress

# ---------------------------------------------------------
# A signal handler that sets global vars according to the
# signal given.
# Unknown signals are ignored.
# ---------------------------------------------------------
sub sigHandler {
	my ($sig) = @_;
	if ( exists $SIGS_CAUGHT{$sig} ) {
		if ( ++$death_note > 5 ) {

			# This is very crude, so only do _THAT_ if everything else failed
			log_error( undef, 'Caught %s Signal %d times - breaking all off!', $sig, $death_note );
			kill 'KILL', $$ or croak('KILL failed');
		} else {
			log_warning( undef, 'Caught %s Signal - Ending Tasks...', $sig );
		}
	} else {
		log_warning( $work_data, 'Caught Unknown Signal [%s] ... ignoring Signal!', $sig );
	}
	return 1;
} ## end sub sigHandler

sub start_capture {
	my ($cmd) = @_;
	my $pid = $$;

	close_standard_io();

	#@type IPC::Shareable
	my $fork_data = IPC::Shareable->new( key => 'WORK_DATA', create => 0 );

	# Wait until we can really start
	wait_for_startup( $pid, $fork_data );

	# Now we can run the command as desired
	log_debug( $fork_data, '%s', join $SPACE, @{$cmd} );
	my @stdout;
	my @stderr;
	my $exc = 0;
	my $exm = $EMPTY;
	my $res = run_cmd_from_fork( $fork_data, $cmd, \@stdout, \@stderr, \$exc, \$exm );

	# We only have to "transport" the results:
	if ( lock_data($fork_data) ) {
		chomp @stdout;
		$fork_data->{PIDs}{$pid}{result}    = join "\n", @stdout;
		$fork_data->{PIDs}{$pid}{exit_code} = $exc;
		$fork_data->{PIDs}{$pid}{error_msg} = $exm;
		unlock_data($fork_data);
	} ## end if ( lock_data($fork_data...))

	# Log everything that has been "caught" in @stderr
	for my $line (@stderr) {
		chomp $line;
		log_error( '%s', $line );
	}

	# This fork is finished now
	set_pid_status( $fork_data, $pid, $FF_FINISHED );

	return $res;
} ## end sub start_capture

sub start_forked {
	my ($cmd) = @_;
	my $pid = $$;

	close_standard_io();

	#@type IPC::Shareable
	my $fork_data = IPC::Shareable->new( key => 'WORK_DATA', create => 0 );

	# Wait until we can really start
	wait_for_startup( $pid, $fork_data );

	# Now we can run the command as desired
	my @stdout;
	my @stderr;
	my $exc = 0;
	my $exm = $EMPTY;

	# Running the command is split out
	my $res = run_cmd_from_fork( $fork_data, $cmd, \@stdout, \@stderr, \$exc, \$exm );

	# We only have to "transport" the results:
	if ( lock_data($fork_data) ) {
		chomp @stderr;
		if ( defined $fork_data ) {
			$fork_data->{PIDs}{$pid}{result}    = join "\n", @stdout;
			$fork_data->{PIDs}{$pid}{exit_code} = $exc;
			$fork_data->{PIDs}{$pid}{error_msg} = ( scalar @stderr > 0 ) ? ( join "\n", @stderr ) : $exm;
		}
		unlock_data($fork_data);
	} ## end if ( lock_data($fork_data...))

	# This fork is finished now
	set_pid_status( $fork_data, $pid, $res ? $FF_FINISHED : $FF_KILLED );

	return $res;
} ## end sub start_forked

# ---------------------------------------------------------
# Start a command asynchronously
# ---------------------------------------------------------
sub start_work {
	my ( $tid, $gid, @cmd ) = @_;
	my $kid = fork;
	( defined $kid ) or croak("Cannot fork()! $!\n");

	# Handle being the fork first
	# =======================================
	if ( 0 == $kid ) {
		start_forked( \@cmd );
		POSIX::_exit(0);  ## Regular exit() would call main::END block
	}

	# === Do the bookkeeping before we return
	# =======================================
	add_pid( $kid, $gid ) and usleep(0);

	# Wait for the fork to mark itself as "created"
	wait_for_pid_status( $kid, $work_data, $FF_CREATED ) and usleep(0);

	# Now the fork waits for the starting signal, let's hand it over
	set_pid_status( $work_data, $kid, $FF_RUNNING );
	log_debug( $work_data, 'Worker %d forked out as PID %d', $tid, $kid );

	return $kid;
} ## end sub start_work

sub start_worker_fork {
	my ( $tid, $gid, $inter_opts ) = @_;

	my $source        = $inter_opts->{'src'};
	my $target        = $inter_opts->{'tgt'};
	my $prgLog        = sprintf $inter_opts->{'prg'}, $tid;
	my $file_from     = sprintf $source_groups{$gid}{$source}, $tid;
	my $file_to       = sprintf $source_groups{$gid}{$target}, $tid;
	my $filter_string = make_filter_string( $gid, $inter_opts );
	my @fps_opts      = ('-fps_mode');
	( 'idn' eq $target ) and ( push @fps_opts, ( 'cfr', '-r', $inter_opts->{'fps'} ) ) or ( push @fps_opts, 'vfr' );
	my @ffargs = (
		$FF,                 @FF_ARGS_START,  '-progress',        $prgLog, ( ( 'guess' ne $audio_layout ) ? qw( -guess_layout_max 0 ) : () ),
		@FF_ARGS_INPUT_INIT, '-i',            $file_from,         @FF_ARGS_ACOPY_FIL, "${B_in}${filter_string}${B_out}",
		@fps_opts,           @FF_ARGS_FORMAT, @FF_ARGS_CODEC_UTV, $file_to
	);

	log_info( $work_data, "Starting Worker %d for:\n%s", $tid + 1, ( join $SPACE, @ffargs ) );
	my $pid = start_work( $tid, $gid, @ffargs );
	( defined $pid ) and ( $pid > 0 ) or croak('BUG! start_work() returned invalid PID!');
	lock_data($work_data);
	@{ $work_data->{PIDs}{$pid}{args} } = @ffargs;
	$work_data->{PIDs}{$pid}{id} = $tid;
	%{ $work_data->{PIDs}{$pid}{interp} } = %{$inter_opts};
	$work_data->{PIDs}{$pid}{prgfile} = $prgLog;
	$work_data->{PIDs}{$pid}{source}  = $source_groups{$gid}{$source};
	$work_data->{PIDs}{$pid}{target}  = $source_groups{$gid}{$target};
	unlock_data($work_data);

	return 1;
} ## end sub start_worker_fork

# --- Second strike, 3 seconds after $thr_progress timeout  ---
# -------------------------------------------------------------
sub strike_fork_kill {
	my ($pid) = @_;

	if ( 0 == reap_pid($pid) ) {
		log_error( $work_data, 'Worker PID %d can not be terminated, trying to KILL...', $pid );
		mark_pid_restart($pid);
		terminator( $pid, 'KILL' );
		( get_pid_status( $work_data, $pid ) < $FF_KILLED ) and set_pid_status( $work_data, $pid, $FF_KILLED );
		return 7;
	} ## end if ( 0 == reap_pid($pid...))

	log_warning( $work_data, 'Fork PID %d is gone! Will restart...', $pid );

	return 13;  # Thread is already gone, start a new one.
} ## end sub strike_fork_kill

# --- Third strike, 6 seconds after $thr_progress timeout  ---
# -------------------------------------------------------------
sub strike_fork_reap {
	my ($pid) = @_;

	while ( can_work && ( 0 == reap_pid($pid) ) ) {
		usleep(50);
	}

	mark_pid_restart($pid);

	return 13;
} ## end sub strike_fork_reap

# --- Last strike, 9 seconds after $thr_progress timeout  ---
# -------------------------------------------------------------
sub strike_fork_restart {
	my ($pid) = @_;
	log_warning( $work_data, 'Re-starting frozen Fork %d', $pid );

	lock_data($work_data);
	my @args       = @{ $work_data->{PIDs}{$pid}{args} };
	my $gid        = $work_data->{PIDs}{$pid}{gid};
	my $inter_opts = $work_data->{PIDs}{$pid}{interp};
	my $prgLog     = $work_data->{PIDs}{$pid}{prgfile};
	my $src        = $work_data->{PIDs}{$pid}{source};
	my $tgt        = $work_data->{PIDs}{$pid}{target};
	my $tid        = $work_data->{PIDs}{$pid}{id};
	unlock_data($work_data);

	# If we have interpolation data, this is an interpolating worker who has their
	# filters to be redone if the 'do_alt' (do alternative interpolation) switches
	# from 0 (libplacebo, might freeze ffmpeg) to 1 (use classic minterpolate)
	if ( defined $inter_opts ) {
		$inter_opts->{'do_alt'} = 1;

		lock_data($work_data);
		my $file_from     = sprintf $work_data->{PIDs}{$pid}{source}, $tid;
		my $file_to       = sprintf $work_data->{PIDs}{$pid}{target}, $tid;
		my $filter_string = make_filter_string( $gid, $inter_opts );
		my @fps_opts      = ('-fps_mode');
		( 'idn' eq $inter_opts->{'tgt'} ) and ( push @fps_opts, ( 'cfr', '-r', $inter_opts->{'fps'} ) ) or ( push @fps_opts, 'vfr' );
		@args = (
			$FF,                 @FF_ARGS_START,  '-progress',        $prgLog, ( ( 'guess' ne $audio_layout ) ? qw( -guess_layout_max 0 ) : () ),
			@FF_ARGS_INPUT_INIT, '-i',            $file_from,         @FF_ARGS_ACOPY_FIL, "${B_in}${filter_string}${B_out}",
			@fps_opts,           @FF_ARGS_FORMAT, @FF_ARGS_CODEC_UTV, $file_to
		);

		# Before we can continue, the old progress file has to be deleted, or handle_fork_progress() might believe the fork
		# already ended, because the old progress file might have a "progress=end" line at the end.
		( -f $prgLog ) and unlink $prgLog;

		unlock_data($work_data);
	} ## end if ( defined $inter_opts)

	log_info( $work_data, "Starting Worker %d for:\n%s", $tid + 1, ( join $SPACE, @args ) );
	my $kid = start_work( $tid, $gid, @args );
	( defined $kid ) and ( $kid > 0 ) or croak('BUG! start_work() returned invalid PID!');
	lock_data($work_data);
	@{ $work_data->{PIDs}{$kid}{args} } = @args;
	$work_data->{PIDs}{$kid}{gid}     = $gid;
	$work_data->{PIDs}{$kid}{prgfile} = $prgLog;
	$work_data->{PIDs}{$kid}{source}  = $src;
	$work_data->{PIDs}{$kid}{target}  = $tgt;
	unlock_data($work_data);

	remove_pid( $pid, 0 );  ## No cleanup, the restarted process will overwrite and we don't want to interfere with the substitute

	return $kid;
} ## end sub strike_fork_restart

# --- First strike after $thr_progress ran out (30 seconds) ---
# -------------------------------------------------------------
sub strike_fork_term {
	my ($pid) = @_;

	if ( 0 == reap_pid($pid) ) {
		log_warning( $work_data, 'Worker PID %d looks frozen...', $pid );
		mark_pid_restart($pid);
		terminator( $pid, 'TERM' );
		( get_pid_status( $work_data, $pid ) < $FF_KILLED ) and set_pid_status( $work_data, $pid, $FF_KILLED );
		return 1;
	} ## end if ( 0 == reap_pid($pid...))

	log_warning( $work_data, 'Fork PID %d is gone! Will restart...', $pid );

	return 13;  # Thread is already gone, start a new one.
} ## end sub strike_fork_term

# ---------------------------------------------------------
# TERM/KILL handler for sending signals to forked children
# ---------------------------------------------------------
sub terminator {
	my ( $kid, $signal ) = @_;

	( defined $kid )    or log_error( $work_data, 'BUG! terminator() called with UNDEF kid argument!' )    and return 0;
	( defined $signal ) or log_error( $work_data, 'BUG! terminator() called with UNDEF signal argument!' ) and return 0;

	if ( !( ( 'TERM' eq $signal ) || ( 'KILL' eq $signal ) ) ) {
		log_error( $work_data, q{Bug: terminator(%d, '%s') called, only TERM and KILL supported!}, $kid, $signal );
		return 0;
	}

	if ( ($kid) > 0 ) {
		log_warning( $work_data, 'Sending %s to pid %d', $signal, $kid );
		if ( kill( $signal, $kid ) > 0 ) {
			usleep(100_000);
			reap_pid($kid);
		} else {
			set_pid_status( $work_data, $kid, $FF_REAPED );
		}
	} else {
		foreach my $pid ( keys %{ $work_data->{PIDs} } ) {
			( $FF_REAPED == get_pid_status( $work_data, $pid ) ) or terminator( $pid, $signal );
		}
	}

	return 1;
} ## end sub terminator

sub unlock_data {
	my ($data) = @_;

	( defined $data ) or return 0;

	#@type IPC::Shareable
	my $lock = tied %{$data};

	( $do_lock_debug > 0 ) and log_debug( $data, '%s <== unlock', get_location(undef) );
	( defined $lock ) and $lock->unlock or return 0;

	return 1;
} ## end sub unlock_data

## @brief Update $work_data->{DEATH} if the main PID has raised $death_note via signal catching
sub update_termination_request {

	( $$ == $main_pid ) or confess("FATAL: update_termination_request() called from PID $$ instead of $main_pid !");

	lock_data($work_data);

	# Has DEATH to be raise?
	( $death_note > $work_data->{DEATH} ) and $work_data->{DEATH} = $death_note;

	# Has DEATH to be reset?
	( 0 == $death_note ) and $work_data->{DEATH} = 0;
	unlock_data($work_data);

	return 1;
} ## end sub update_termination_request

# A warnings handler that lets perl warnings be printed via log
sub warnHandler {
	my ($warn) = @_;
	return log_warning( undef, '%s', $warn );
}

sub validate_input_file {
	my ($src) = @_;
	if ( -f $src ) {
		my $in_size = -s $src;
		( $in_size > 0 ) or log_error( $work_data, "Input file '%s' is empty!", $src ) and return 0;
		++$source_count;
	} else {
		log_error( $work_data, "Input file '%s' does not exist!", $src );
		return 0;
	}
	return 1;
} ## end sub validate_input_file

sub wait_for_all_forks {
	my $result = 1;

	lock_data($work_data);
	my @PIDs = ( sort keys %{ $work_data->{PIDs} } );
	unlock_data($work_data);

	log_debug( $work_data, 'Waiting for %d PIDs to end...', scalar @PIDs );

	foreach my $pid (@PIDs) {
		my $dsecs = 0;

		# Wait for PID
		while ( 0 == reap_pid($pid) ) {
			usleep(100_000);  # Poll 10 times per second
			                  # TERM after 3, 4, 5, 6, 7 seconds, and KILL after 10 seconds.
			if ( 0 == ( ++$dsecs % 10 ) ) {
				( $dsecs >= 30 ) and ( $dsecs <= 70 ) and terminator( $pid, 'TERM' )
				  or ( 100 <= $dsecs )
				  and terminator( $pid, 'KILL' );
			}
		} ## end while ( 0 == reap_pid($pid...))

		remove_pid( $pid, 1 ) or $result = 0;

	} ## end foreach my $pid (@PIDs)

	log_debug( $work_data, "All PIDs ended '%s'.", ( $result > 0 ) ? 'successfully' : 'with errors!' );

	return $result;
} ## end sub wait_for_all_forks

sub wait_for_capture {
	my ($kid) = @_;

	wait_for_pid_status( $kid, $work_data, $FF_CREATED );
	set_pid_status( $work_data, $kid, $FF_RUNNING );
	log_debug( $work_data, 'Process %d forked out', $kid );

	# Now wait for the result
	wait_for_pid_status( $kid, $work_data, $FF_FINISHED );

	return 1;
} ## end sub wait_for_capture

sub wait_for_pid_status {
	my ( $pid, $fork_data, $status ) = @_;

	lock_data($fork_data);
	my $stLoc = get_location(undef);
	log_debug( $fork_data, '%s: Fork Status %d / %d', $stLoc, get_pid_status( $fork_data, $pid ), $status );
	unlock_data($fork_data);

	usleep(1);  ## A little "yield()" simulation
	while ( $status > get_pid_status( $fork_data, $pid ) ) {
		usleep(500);  # poll each half millisecond
		log_debug( $fork_data, '%s: Fork Status %d / %d', $stLoc, get_pid_status( $fork_data, $pid ), $status );
	}
	log_debug( $fork_data, '%s: Fork Status %d / %d reached -> ending wait', $stLoc, get_pid_status( $fork_data, $pid ), $status );

	return 1;
} ## end sub wait_for_pid_status

sub wait_for_startup {
	my ( $pid, $fork_data ) = @_;

	log_debug( $fork_data, 'Have data? %s', ( defined($fork_data) && defined( $fork_data->{PIDs}{$pid} ) ) ? 'yes' : 'no' );

	# Wait until the work data is initialized
	while ( !pid_exists( $fork_data, $pid ) ) {
		usleep(500);  # poll each half millisecond
		log_debug( $fork_data, 'Have data? %s', pid_exists( $fork_data, $pid ) ? 'yes' : 'no' );
	}

	# Now we can tell the world that we are created
	set_pid_status( $fork_data, $pid, $FF_CREATED );
	log_debug( $fork_data, 'Have data? yes (Status %d)', get_pid_status( $fork_data, $pid ) );
	usleep(1);

	# Wait until we got started
	return wait_for_pid_status( $pid, $fork_data, $FF_RUNNING );
} ## end sub wait_for_startup

# This is a watchdog function that displays progress and joins all threads nicely if needed
sub watch_my_forks {
	lock_data($work_data);
	my $result       = 1;
	my %fork_timeout = ();
	my %fork_strikes = ();
	my @PIDs         = sort keys %{ $work_data->{PIDs} };
	log_debug( $work_data, 'Forks : %s (%d active)', ( join ', ', keys %{ $work_data->{PIDs} } ), $work_data->{cnt} );
	unlock_data($work_data);

	my $forks_active = initialize_fork_watch( \%fork_strikes, \%fork_timeout, @PIDs );

	# Now check on all forks periodically until all are gone
	while ( $forks_active > 0 ) {
		my $pids_crashed = 0;
		my %prgData      = (
			bitrate     => 0.0,
			drop_frames => 0,
			dup_frames  => 0,
			fps         => 0.0,
			frame       => 0,
			out_time_ms => 0,
			total_size  => 0
		);

		# If the main program was signalled to leave, transfer the message
		update_termination_request();

		lock_data($work_data);
		my $fork_cnt = $work_data->{cnt};
		unlock_data($work_data);
		$forks_active = 0;

		can_work or last;

		foreach my $pid (@PIDs) {
			my $fork_status = handle_fork_progress( $pid, \%prgData, \%fork_timeout );
			( $fork_status < 0 ) and ++$pids_crashed        # The fork has crashed or broken off with an error
			  or ( $fork_status > 0 ) and ++$forks_active;  # The fork is still running
			usleep(0);

			# Make sure we later know how many frames got dropped/dup'd.
			my $dropdups = $prgData{drop_frames} + $prgData{dup_frames};
			my $gid      = $work_data->{PIDs}{$pid}{gid};

			# If a PID got just restarted, the gid might not have been set, yet, so better check it.
			# (This is rare. Locking/Unlocking work_data for every request is too much overhead.)
			if ( defined $gid ) {
				( defined $source_groups{$gid}{dropdups} ) and ( $source_groups{$gid}{dropdups} >= $dropdups )
				  or $source_groups{$gid}{dropdups} = $dropdups;
			}
		} ## end foreach my $pid (@PIDs)

		# Ensure that we do not tear everything down if a PID looks crashed. Check it again first
		if ( $pids_crashed > 0 ) {
			log_debug( $work_data, 'Found %d suspicious forks, checking status...', $pids_crashed );
			lock_data($work_data);
			@PIDs = sort keys %{ $work_data->{PIDs} };
			unlock_data($work_data);
			$ret_global = ( check_pids_crashed(@PIDs) > 0 ) ? 23 : 0;
			lock_data($work_data);
			$fork_cnt = $work_data->{cnt};
			unlock_data($work_data);
		} ## end if ( $pids_crashed > 0)

		# Now handle progress data
		( $forks_active > 0 ) or show_progress( $fork_cnt, $forks_active, \%prgData, 1 ) and next;
		show_progress( $fork_cnt, $forks_active, \%prgData, 0 );
		can_work or send_forks_the_kill();

		lock_data($work_data);
		@PIDs = sort keys %{ $work_data->{PIDs} };
		unlock_data($work_data);

		foreach my $pid (@PIDs) {
			can_work or last;
			pid_exists( $work_data, $pid )
			  or pid_shall_restart( $work_data, $pid )
			  or log_debug( '(BUG???) PID %d neither exists nor shall restart. Why is it still in the list, then?', $pid )
			  and next;
			handle_fork_strikes( $pid, \%fork_timeout, \%fork_strikes );
		} ## end foreach my $pid (@PIDs)

		usleep(500_000);
	} ## end while ( $forks_active > 0)

	$result = wait_for_all_forks();

	return $result;
} ## end sub watch_my_forks

sub write_to_console {
	my ($msg) = @_;

	if ( $have_progress_msg > 0 ) {
		print "\n";
		$have_progress_msg = 0;
	}

	local $| = 1;
	return print "${msg}\n";
} ## end sub write_to_console

sub write_to_log {
	my ($msg) = @_;

	if ( open my $fLog, '>>', $logfile ) {
		print {$fLog} "${msg}\n";
		close $fLog or confess("Closing logfile '$logfile' FAILED!");
	}

	return 1;
} ## end sub write_to_log

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

=item B<--maxfps>

The maximum FPS to upscale the video to. Must be larger than the target FPS and
defaults to twice the target FPS. If the FPS of the source material is higher
than the set maximum FPS, the source FPS will be used instead.

=item B<--targetfps>

The target FPS to produce the target video in. Defaults to 60 FPS for all videos
that are made from source material of 50+ FPS, and to 30 FPS for all videos with
lower source FPS. If it is set to a larger number than the set maximum FPS, it
will be lowered to the maximum FPS set.
The lower bound is 1, any value below 1 is ignored.

=item B<-t | --tempdir>

Path to the directory where the temporary files are written. Defaults to the
directory of the input file(s). Ensure to have 80x the space of the input!

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
to twice (*) the target FPS in a first step, then do another search for duplicate
frames and interpolate down the the target FPS.

If the source has at least 50 FPS in average, the target is set to 60 FPS. For
sources with less than 50 FPS in average, the target is set to 30 FPS.
You can use the -u/--upgrade option to force the target to be 60 FPS, no matter
the source average.

(*): If the source video has more than twice the target FPS, it will not be down-
     scaled, but the source fps will be kept for the first interpolation.

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

=item B<--lock-debug>

If --debug is used, this switch enables lock messages whenever the central data
structure is un-/locked. This produces an enormous amount of lines in the log
file, so use it with care!
This switch is ignored unless -D/--debug is also used.

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

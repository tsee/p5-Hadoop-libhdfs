use 5.008005;
use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
# my $includes = "-I/usr/lib/jvm/java-1.6.0-sun-1.6.0.11/include -I/usr/lib/jvm/java-1.6.0-sun-1.6.0.11/include/linux -I.";
# my $libs = "-L/usr/lib/jvm/java-1.6.0-sun-1.6.0.11/jre/lib/amd64/server/ -ljvm -lhdfs";
my $includes = "-I/usr/lib/jvm/java-1.6.0-sun-1.6.0.32.x86_64/include -I/usr/lib/jvm/java-1.6.0-sun-1.6.0.32.x86_64/include/linux -I.";
my $libs = "-L/usr/lib/jvm/java-1.6.0-sun-1.6.0.32.x86_64/jre/lib/amd64/server/ -ljvm -lhdfs";
WriteMakefile(
    NAME              => 'Hadoop::libhdfs',
    VERSION_FROM      => 'lib/Hadoop/libhdfs.pm', # finds $VERSION
    PREREQ_PM         => {}, # e.g., Module::Name => 1.1
    ($] >= 5.005 ?     ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM  => 'lib/Hadoop/libhdfs.pm', # retrieve abstract from module
       AUTHOR         => 'Steffen Mueller <smueller@cpan.org>') : ()),
    LIBS              => [$libs], # e.g., '-lm'
    DEFINE            => '', # e.g., '-DHAVE_SOMETHING'
    INC               => $includes, # e.g., '-I. -I/usr/include/other'
	# Un-comment this if you add C files to link with later:
    OBJECT            => '$(O_FILES)', # link all the C files too
);
